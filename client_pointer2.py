import chord
import clientxmlrpc
import random
from stabilizer import Stabilizer
from key import Key, Uid
import numpy as np
import collections
import networkx as nx
import copy
import threading
import subprocess
import sys
import time,datetime
from collections import deque,defaultdict
import logging
import xmlrpc.client
import socket
from clientxmlrpc import ChordClientxmlrpcProxy, TimeoutTransport, DEFAULT_CONNECTION_TIMEOUT
from concurrent.futures import ThreadPoolExecutor, as_completed


# ログ出力抑制: 最後に集計だけを出す
VERBOSE_RUNTIME_LOG = False

# 失敗種別の総数カウント
ERR_COUNTS = defaultdict(int)

# 各種別のサンプルを少数だけ保存（最後に表示）
MAX_SAMPLES_PER_KIND = 10
ERR_SAMPLES = defaultdict(lambda: deque(maxlen=MAX_SAMPLES_PER_KIND))

_proxy_cache = {}

# 離脱ノード数ごとの統計
# key: num_departed_nodes (0,1,2,...)
CHURN_BUCKET_STATS = defaultdict(lambda: {
    "put_ok": 0,
    "put_fail": 0,
    "get_ok": 0,
    "get_fail": 0,
    "put_attempts": 0,
    "get_attempts": 0,
})

BUCKET_SECONDS = 60

TIME_BUCKET_STATS = defaultdict(lambda: {
    "put_ok": 0,
    "put_fail": 0,
    "get_ok": 0,
    "get_fail": 0,
    "put_attempts": 0,
    "get_attempts": 0,
})

# transfer_stats の「最後に観測できた値」(CRASH 対応)
# key: node_uid(str)  value: {"ts": float, "stats": dict}
LAST_SEEN_TRANSFER_STATS = {}

# ===== churnイベントの時刻記録 =====
# 例: {"t": 123.4, "action": "DEPART"/"JOIN", "ip": "...", "port": 17000, "departed_after": 2}
CHURN_EVENTS = []

# 実験開始時刻（mainでセット）
EXPERIMENT_T0 = None

MAX_PUT_WORKERS = 32
MAX_GET_WORKERS = 32

RPC_RETRIES_CLIENT = 3
RPC_BACKOFF_CLIENT = 0.2

def record_err(kind: str, msg: str = ""):
    ERR_COUNTS[kind] += 1
    if msg:
        # サンプルは少数だけ保持
        if len(ERR_SAMPLES[kind]) < MAX_SAMPLES_PER_KIND:
            ERR_SAMPLES[kind].append(msg)

def record_info(msg: str):
    if VERBOSE_RUNTIME_LOG:
        print(msg)

def current_departed_count():
    with CHURN_LOCK:
        return len(DEPARTED_NODES)

def get_raw_serverproxy(ip: str, port: int, timeout: float = DEFAULT_CONNECTION_TIMEOUT):
    url = f"http://{ip}:{port}"
    transport = TimeoutTransport(timeout=timeout)
    return xmlrpc.client.ServerProxy(url, transport=transport, allow_none=True)

def raw_get_content(ip: str, port: int, uid: str, timeout: float = DEFAULT_CONNECTION_TIMEOUT):
    p = get_raw_serverproxy(ip, port, timeout=timeout)
    # ここは例外を握りつぶさない（socket.timeout / Fault がそのまま出る）
    return p.get_content(uid)

def get_proxy(ip: str, port: int, timeout: float = None):
    """clientxmlrpc.py と同じ流儀で /chord に接続する Proxy を返す（キャッシュ付き）"""
    if timeout is None:
        timeout = DEFAULT_CONNECTION_TIMEOUT
    key = (ip, port, timeout)
    p = _proxy_cache.get(key)
    if p is None:
        p = ChordClientxmlrpcProxy(ip, port, timeout=timeout)
        _proxy_cache[key] = p
    return p

def _now_rel():
    """実験開始からの経過秒 (float)"""
    if EXPERIMENT_T0 is None:
        return None
    return time.time() - EXPERIMENT_T0

def _time_bucket_index():
    """現在時刻が属する60秒窓のindex (0,1,2,...)"""
    t = _now_rel()
    if t is None:
        return 0
    return int(t // BUCKET_SECONDS)

def _record_churn_event(action: str, ip: str, port: int):
    """churnイベントを記録（CHURN_LOCKの内側で呼ぶ想定）"""
    t = _now_rel()
    departed_after = len(DEPARTED_NODES)
    CHURN_EVENTS.append({
        "t": float(t) if t is not None else -1.0,
        "action": action,
        "ip": ip,
        "port": int(port),
        "departed_after": int(departed_after),
    })

def get_transfer_stats(self):
    """Return transfer stats (primitive dict) for client aggregation."""
    try:
        with self._transfer_lock:
            return dict(self.transfer_stats)
    except Exception:
        return {}

def collect_transfer_stats(all_servers):
    """all_servers: list of (ip, port) or server dicts どちらでも対応させる"""
    agg = {}
    per_node = {}
    alive_nodes = []

    def _add(dst, k, v):
        try:
            dst[k] = int(dst.get(k, 0)) + int(v)
        except Exception:
            pass

    for s in all_servers:
        try:
            if isinstance(s, dict):
                ip, port = s["ip"], int(s["port"])
            else:
                ip, port = s[0], int(s[1])

            node_uid = str(get_uid(ip, port))
            proxy = clientxmlrpc.create_proxy(ip, port, timeout=3)
            st = proxy.get_transfer_stats()
            if not isinstance(st, dict):
                continue

            now = time.time()
            per_node[node_uid] = {
                "source": "fresh",
                "ts": now,
                "stats": dict(st),
                "ip": ip,
                "port": port,
            }
            # CRASH対策キャッシュ更新
            LAST_SEEN_TRANSFER_STATS[node_uid] = {"ts": now, "stats": dict(st)}
            alive_nodes.append((ip, port, node_uid))

            for k, v in st.items():
                _add(agg, k, v)
        except Exception:
            continue

    # 2) 生存ノードが保持している「離脱ノードのアーカイブ」を収集して統合（最新tsを優先）
    archive_seen = {}  # departed_uid -> {"ts": float, "stats": dict}
    for ip, port, _ in alive_nodes:
        try:
            proxy = clientxmlrpc.create_proxy(ip, port, timeout=3)
            snap = proxy.get_departed_transfer_stats_snapshot()
            if not isinstance(snap, dict):
                continue
            for departed_uid, payload in snap.items():
                try:
                    if not isinstance(payload, dict):
                        continue
                    ts = float(payload.get("ts", -1.0))
                    st = payload.get("stats", {})
                    if not isinstance(st, dict):
                        continue
                    prev = archive_seen.get(departed_uid)
                    if (prev is None) or (ts > float(prev.get("ts", -1.0))):
                        archive_seen[departed_uid] = {"ts": ts, "stats": dict(st)}
                except Exception:
                    continue
        except Exception:
            continue

    # 3) 非生存ノードについて、archive→last-seen の順で埋める（表示用）
    for s in all_servers:
        try:
            if isinstance(s, dict):
                ip, port = s["ip"], int(s["port"])
            else:
                ip, port = s[0], int(s[1])
            node_uid = str(get_uid(ip, port))
            if node_uid in per_node:
                continue  # freshあり

            # archive があれば採用
            if node_uid in archive_seen:
                payload = archive_seen[node_uid]
                st = payload.get("stats", {}) if isinstance(payload, dict) else {}
                per_node[node_uid] = {
                    "source": "archive",
                    "ts": payload.get("ts"),
                    "stats": dict(st) if isinstance(st, dict) else {},
                    "ip": ip,
                    "port": port,
                }
                # agg に加算
                for k, v in (st or {}).items():
                    _add(agg, k, v)
                continue

            # last-seen があれば採用（CRASH想定）
            if node_uid in LAST_SEEN_TRANSFER_STATS:
                payload = LAST_SEEN_TRANSFER_STATS[node_uid]
                st = payload.get("stats", {}) if isinstance(payload, dict) else {}
                per_node[node_uid] = {
                    "source": "last_seen",
                    "ts": payload.get("ts"),
                    "stats": dict(st) if isinstance(st, dict) else {},
                    "ip": ip,
                    "port": port,
                }
                for k, v in (st or {}).items():
                    _add(agg, k, v)
                continue

            # 何も無い
            per_node[node_uid] = {
                "source": "missing",
                "ts": None,
                "stats": {},
                "ip": ip,
                "port": port,
            }
        except Exception:
            continue

    return agg, per_node, archive_seen


'''
chordのサーバはもう立ってる状態
グラフカバー率、hop数、実行時間について①1個ずつ聞く②隣接を辿る③ExFingersを使う　で比較
クエリの順番はランダムでいい.
パラメータ：
・サーバが経路キャッシュを保持する確率p
・exFingersの平均長n
'''

class NodeStats:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.is_up = False
        self.up_since = None
        self.uptime_total = 0.0
        self.reset_events = 0

        # method -> int
        self.counts_total = defaultdict(int)
        self.counts_last = None  # dict or None

        self.lock = threading.Lock()
        self.last_seen_ok = None

    def mark_up(self, now=None):
        now = now or time.time()
        with self.lock:
            if self.is_up:
                return
            self.is_up = True
            self.up_since = now
            self.last_seen_ok = now

    def mark_down(self, now=None):
        now = now or time.time()
        with self.lock:
            if not self.is_up:
                return
            if self.up_since is not None:
                self.uptime_total += max(0.0, now - self.up_since)
            self.is_up = False
            self.up_since = None

    def add_counts_snapshot(self, snap: dict):
        """snap は get_message_counts の dict を想定。差分で total に積む。"""
        if not isinstance(snap, dict):
            return
        with self.lock:
            if self.counts_last is None:
                # 初回は差分を取れないので last を置くだけ（必要なら初回分も加算してOK）
                self.counts_last = dict(snap)
                return
            for k, v in snap.items():
                if isinstance(v, int):
                    prev = self.counts_last.get(k, 0)
                    if isinstance(prev, int):
                        delta = v - prev
                        if delta >= 0:
                            self.counts_total[k] += delta
                        else:
                            # カウンタが減った=プロセス再起動/リセットの可能性 
                            self.reset_events += 1 
                            self.counts_total[k] += v

            self.counts_last = dict(snap)

    def finalize(self, now=None):
        """実験終了時：UPなら uptime を閉じる"""
        now = now or time.time()
        with self.lock:
            if self.is_up and self.up_since is not None:
                self.uptime_total += max(0.0, now - self.up_since)
                self.is_up = False
                self.up_since = None

NODE_STATS = {}  # (ip, port) -> NodeStats


log = logging.getLogger('chordLog')
log.setLevel(logging.DEBUG)
if not any(isinstance(handler, logging.StreamHandler) for handler in log.handlers):
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(' %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)

# ---- get_content retry settings ----
# NOTE: get_content のリトライは Chord 内部RPCを増やしやすいので、まずは小さめ推奨。
ENABLE_GET_CONTENT_RETRIES = True
GET_CONTENT_RETRIES = 3
GET_CONTENT_BACKOFF = 0.3  # seconds; actual sleep is backoff * attempt
GET_CONTENT_RETRY_ON_NONE = False


stats = {
    "put_ok": 0,
    "put_fail": 0,
    "get_ok": 0,
    "get_fail": 0,
}
put_attempts = 0
get_attempts = 0

def generate_synthetic_contents(num_items: int, gid: str):
    """
    neighbors を使わない人工データを作る
    返り値 : list[dict]
    """
    contents = []
    now = int(time.time())
    for i in range(num_items):
        raw = f"{gid}:{now}:{i}:{random.randint(0, 1_000_000)}"
        uid = Uid(raw).value
        contents.append({
            "gid": gid,
            "id": i,
            "data": f"payload-{i}",
            "uid": uid,
            "neighbors": None,
        })
    return contents

def _safe_rpc_call(method_callable, *args, _raise_on_error=False, **kwargs):
    """
    client_pointer2.py用の安全なRPC呼び出しラッパー
    method_callable: proxy.some_method のような「呼べるオブジェクト」を渡す
    戻り値: 通常はmethod_callable(*args, **kwargs)の結果，失敗したらNoneを返す
    互換性のため_raise_on_errorを受け取り，内部でproxyのラッパー向けに渡す
    """
    # 1) ノード停止・closing 中は即落とす
    if getattr(self, "_stop_event", None) is not None and self._stop_event.is_set():
        return None
    if getattr(self, "_rpc_server", None) is not None and self._rpc_server.is_closing():
        return None

    # 2) executor が死んでるなら submit しない（ログだけ出して return）
    if _RPC_EXECUTOR is None or getattr(_RPC_EXECUTOR, "_shutdown", False):
        log.warning("[EXECUTOR_ALREADY_SHUTDOWN] pid=%s thread=%s shutting=%s",
                    os.getpid(), threading.current_thread().name, _RPC_SHUTTING_DOWN)
        return None
    try:
        return method_callable(*args, _raise_on_error=_raise_on_error, **kwargs)
    except TypeError as e:
        log.warning(f"_safe_rpc_call caught exception calling {getattr(method_callable, '__name__', str(method_callable))}: {e}")
        if _raise_on_error:
            raise
        return None
    except RuntimeError as e:
        msg = str(e)
        if "cannot schedule new futures after shutdown" in msg:
            # ★フラグが立ってないなら、外部経路の shutdown を疑う
            if not _RPC_SHUTTING_DOWN:
                set_rpc_shutting_down("executor_shutdown_detected_in_safe_rpc_call")
            log.warning("[EXECUTOR_ALREADY_SHUTDOWN] pid=%s thread=%s shutting=%s exec_id=%s\n%s",
                        os.getpid(), threading.current_thread().name,
                        _RPC_SHUTTING_DOWN, id(_RPC_EXECUTOR),
                        "".join(traceback.format_stack(limit=25)))
            return None
        raise
    except Exception as e:
        log.warning(f"_safe_rpc_call caught exception calling {getattr(method_callable, '__name__', str(method_callable))}: {e}")
        if _raise_on_error:
            raise
        return None

def call_with_retries(method_callable, *args, retries=3, backoff=1, retry_on_none=False):
    """
    RPCに対して再試行を行うヘルパー
    retries: 試行回数
    backoff: sleepのベース
    戻り値: 成功時の結果，全失敗でNone
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            res = method_callable(*args)  
            if res is None and retry_on_none:
                log.debug(f"RPC {getattr(method_callable, '__name__', str(method_callable))} returned None on attempt {attempt} (retry_on_none=True)")
                # 明示的に次の attempt へ
                if attempt < retries:
                    time.sleep(backoff * attempt)
                continue
            else:
                return res
        except xmlrpc.client.Fault as e:
            # アプリ側のバグ/不正引数 (KeyError, TypeError 等) はリトライしても直らないので即座に返す
            msg = str(e.faultString)
            if ("KeyError" in msg) or ("TypeError" in msg) or ("unexpected keyword argument" in msg):
                raise
            last_exc = e
            log.debug(
                f"RPC {getattr(method_callable,'__name__',method_callable)} fault "
                f"on attempt {attempt}/{retries}: code={e.faultCode} msg={msg[:200]}"
            )
        except (socket.timeout, ConnectionError, OSError) as e:
            last_exc = e
            log.debug(
                f"RPC {getattr(method_callable,'__name__',method_callable)} network error "
                f"on attempt {attempt}/{retries}: {type(e).__name__}: {e}"
            )
        except Exception as e:
            last_exc = e
            log.warning(
                f"RPC {getattr(method_callable,'__name__',method_callable)} "
                f"failed on attempt {attempt}/{retries}: {e!r}"
            )
            # 必要なら traceback.format_exc() も出す
        if attempt < retries:
            time.sleep(backoff * attempt)

    if last_exc is not None:
        raise RuntimeError(
            f"RPC {getattr(method_callable,'__name__',method_callable)} failed after {retries} attempts"
        ) from last_exc
    return None


def stop_all_server(servers):
    for ip,port in servers:
        clientxmlrpc.ChordClientxmlrpcProxy(ip, port).stop() #RPCではどのクラスの関数かは意識されない
        
def generate_random_normal(n):
    """平均nの正規分布に従い、０以上の整数を返す

    Args:
        n (int,float): 正規分布の

    Returns:
        _type_: _description_
    """    
    std_dev=n/10 #+-10%    
    sample = np.random.normal(n, scale=std_dev)
    sample = np.round(sample).astype(int) #丸め込む
    sample = np.clip(sample, 0, None)  # 負の値を0にする
    return sample

def get_uid(ip,port):
    '''
    ipとportを受け取りUid型を返す
    '''
    return Uid(ip + ":" + repr(port))

def _uid8(x) -> str:
    """c が dict でも Contents でも安全に uid(先頭8) を取り出す。"""
    try:
        # dict ACK / dict content
        if isinstance(x, dict):
            u = x.get("uid") or x.get("key") or x.get("id")
            if u is None:
                return "????????"
            return str(u)[:8]

        # Contents-like object: c.uid.value
        uobj = getattr(x, "uid", None)
        if uobj is not None:
            v = getattr(uobj, "value", None)
            if v is not None:
                return str(v)[:8]
            return str(uobj)[:8]

        # fallback
        return str(x)[:8]
    except Exception:
        return "????????"




def _normalize_get_content_response(resp):
    """
    chord.py の get_content は、
      - hit : (list_of_contents_dict, exhop:int, exrhop:int)
      - miss: (hop:int, rHop:int)
    を返しうるので正規化する。
    戻り値: (contents_list_or_None, hop:int|None, rhop:int|None, exhop:int|None, exrhop:int|None)
    """
    if resp is None:
        return (None, None, None, None, None)

    if isinstance(resp, tuple):
        if len(resp) == 2 and all(isinstance(x, int) for x in resp):
            hop, rHop = resp
            return (None, hop, rHop, None, None)
        if len(resp) == 3:
            contents, exhop, exrhop = resp
            # contents は list[dict] のはず
            if isinstance(contents, list):
                return (contents, None, None, exhop, exrhop)
            # 変な形
            return (None, None, None, None, None)

    # 予期しない型
    return (None, None, None, None, None)


def create_all_exFinger_table_no_dup(servers:list,n:int):
    '''
    ループしないようすべてのサーバのexFingersを設定。クライアント主導．RPCでやる.
    Args
        servers(list):全サーバのリスト
        n(int): 平均のexFingersの長さ
    Returns
        nodeIFs(list):もったいないので全サーバのNodeInterface型のリスト
    '''
    nodeIFs=[]
    exfs=generate_dag_with_average_outdegree(servers,n)
    for src,dst in exfs.items():
        #print(src)
        #print(dst)
        cNode=chord.NodeInterface({'ip':src[0],'port':src[1]})
        #cNode.methodProxy.create_exFinger_table()
        cNode.methodProxy.set_exFinger_table(dst,True)
        nodeIFs.append(cNode)
    print("all exFingers are initialized")
    return nodeIFs



def remove_duplicate_dicts(list_of_dicts,key):
    unique_dicts = {}
    for d in list_of_dicts:
        unique_dicts[d[key]] = d # 'id' をキーとして辞書を保存。重複したidがあれば上書きされる
    return list(unique_dicts.values())

# サーバー接続情報（ユーザー名とパス）
SSH_USER = 'yuushi'
# 秘密鍵のパスは、SSH接続自体が機能しているため、ここでは明示的なパスは省略（環境変数に依存）
# ただし、subprocessで実行する場合は、鍵の指定が必要なことが多いです。ここでは、
# ユーザーが提供した「ssh user_name@[IPアドレス]で接続できる」という条件を前提とし、鍵なしで試みます。
REMOTE_CHORD_PATH = '~/pychord-master' # サーバー上のchord.pyのディレクトリ

# チャーンの状態を管理するグローバル変数
CHURN_LOCK = threading.Lock()
# {IP: {"port": int, "mode": "DEPART"|"CRASH"}} の形で、現在離脱中のノードを管理
# "DEPART": RPC stop()による　
# "CRASH": SIGKILL (通知，handoffなし)
DEPARTED_NODES = {}
MAX_CHURN = 2 # 同時離脱の最大数

# tmuxペイン紐付け
TMUX_PANES = {
    "172.16.1.11": "eva-test:1.0",
    "172.16.2.11": "eva-test:1.1",
    "172.16.3.11": "eva-test:1.2",
    "172.16.2.12": "eva-test:1.3",
    "172.16.1.12": "eva-test:1.4",
    "172.16.3.12": "eva-test:1.5",
    "172.16.4.11": "eva-test:2.0",
    "172.16.4.12": "eva-test:2.1",
    "172.16.8.11": "eva-test:2.2",
    "172.16.5.11": "eva-test:2.3"
}

# CRASHのためのtmuxペイン紐付け
TMUX_PANES_CRASH = {
    "172.16.1.11":  "eva-test:5.0",  # eva1
    "172.16.2.11":  "eva-test:5.1",  # eva2
    "172.16.3.11":  "eva-test:5.2",  # eva3
    "172.16.2.12":  "eva-test:5.3",  # eva4
    "172.16.1.12":  "eva-test:5.4",  # eva5
    "172.16.3.12":  "eva-test:5.5",  # eva6

    "172.16.4.11":  "eva-test:5.6",  # eva7
    "172.16.4.12":  "eva-test:5.9",  # eva10
    "172.16.8.11":  "eva-test:5.12",  # eva13
    "172.16.5.11":  "eva-test:5.7"  # eva8
}

def crash_remote_node(ip, pattern=None):
    """
    CRASH: chord.py プロセスをSIGKILLで落とす (通知なし・handoffなし)
    - tmux pane へpkill/killを送るだけで, RPCは一切呼ばない
    - paneは残す （後で同じ pane で python3 chord.py ... を再起動できる）
    """
    pane_name = TMUX_PANES_CRASH.get(ip)
    if not pane_name:
        print(f"ERROR: No tmux pane defined for IP {ip}. Skipping crash.")
        return False

    # chord.py は引数に ip を取って起動されている前提なので、
    # "python3 chord.py <ip>" にマッチさせて落とすのが安全。
    # もし手元の実際の起動コマンドが違う場合は pattern を上書きできるようにしてある。
    if pattern is None:
        pattern = f"python3 chord.py {ip}"

    # pkill -9 -f "<pattern>"
    # -f: full command line match
    # -9: SIGKILL
    kill_cmd = f"pkill -9 -f \"{pattern}\""

    # pane上で実行
    cmd = f"tmux send-keys -t {pane_name} '{kill_cmd}' Enter"
    try:
        subprocess.run(cmd, shell=True, timeout=5, check=True)
        print(f"--- Node {ip} CRASH (SIGKILL) issued in tmux pane {pane_name} ---")
        return True
    except Exception as e:
        print(f"WARNING: Failed to CRASH chord.py on {ip}: {e}")
        return False

def wait_until_down(ip, port, timeout=5.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            p = clientxmlrpc.ChordClientxmlrpcProxy(ip, port, timeout=1)
            # 何でもいいので軽いRPCを投げる（get_successor など）
            p.get_successor()
            time.sleep(0.1)
        except Exception:
            return True
    return False


def stop_remote_node(ip):
    """リモートサーバ上のchord.pyプロセスを強制停止"""
    pane_name = TMUX_PANES.get(ip)
    if not pane_name:
        print(f"ERROR: No tmux pane defined for IP {ip}. Skipping restart.")
        return False
    
    cmd = f"tmux send-keys -t {pane_name} C-c"
    try:
        subprocess.run(cmd, shell=True, timeout=5, check=True)
        print(f"--- Node {ip} chord.py forcibly stopped ---")
        return True
    except Exception as e:
        print(f"WARNING: Failed to stop chord.py on {ip}: {e}")
        return False

def restart_node_in_tmux_pane(ip, port):
    """
    離脱ノードを再起動する. tmuxペイン内でchord.pyを実行
    """
    pane_name = TMUX_PANES.get(ip)
    if not pane_name:
        print(f"ERROR: No tmux pane defined for IP {ip}. Skipping restart.")
        return False
    
    if pane_name == "eva:1.1":
        cmd = f"tmux send-keys -t {pane_name} 'python3 chord.py {ip} 0' Enter"
    else:
        cmd = f"tmux send-keys -t {pane_name} 'python3 chord.py {ip} 1' Enter"

    try:
        subprocess.run(cmd, shell=True, check=True)
        print(f"--- Node {ip}:{port} rejoining via tmux pane {pane_name} ---")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to restart node {ip}:{port} in pane {pane_name}. {e}")
        return False

def induce_churn_event(all_servers):
    """ノードの離脱または再参加をランダムに実行する"""
    global DEPARTED_NODES
    
    with CHURN_LOCK:
        active_nodes = [s for s in all_servers if s[0] not in DEPARTED_NODES]
        
        # 0. 実行可能な操作だけを選ぶ（no-op を禁止）
        choices = []
        if DEPARTED_NODES:
            choices.append('JOIN')
        if active_nodes and (len(DEPARTED_NODES) < MAX_CHURN):
            choices.extend(['DEPART', 'CRASH'])
        if not choices:
            return False
        action = random.choice(choices)
        
        # 1. ノードの再参加 (JOIN) - 離脱済みノードが存在し、かつアクションがJOINの場合
        if DEPARTED_NODES and (action == 'JOIN' or not active_nodes):
            ip, info = random.choice(list(DEPARTED_NODES.items()))
            if isinstance(info, dict):
                port = int(info.get("port", 17000))
                mode = str(info.get("mode", "DEPART"))
            else:
                port = int(info)
                mode = "DEPART"
            
            # リモートで chord.py を再実行 (ブートストラップノード経由でJoin)
            success = restart_node_in_tmux_pane(ip, port)
            if success:
                del DEPARTED_NODES[ip]
                _record_churn_event("JOIN", ip, port)
                # どのモードから復帰したか
                print(f"--- Node {ip}:{port} JOIN after {mode} ---")
                after_rejoin(ip, port)
                return True
            else:
                print(f"WARNING: Could not restart node {ip}:{port}. Keeping it in DEPARTED_NODES.")
                return False

        # 2. ノードの離脱 (DEPART/CRASH) - 離脱数が最大以下で、アクティブノードが存在し、かつアクションがDEPARTの場合
        elif len(DEPARTED_NODES) < MAX_CHURN and active_nodes and action in ('DEPART', 'CRASH'):
            # アクティブなノードの中からランダムに選び、離脱させる
            ip, port = random.choice(active_nodes)
            
            if action == 'DEPART':
                # RPC経由でノードを停止（※現状 stop_with_stats は未接続とのこと）
                try:
                    proxy = clientxmlrpc.ChordClientxmlrpcProxy(ip, port, timeout=3)
                    proxy.stop()  # LocalNode.stop() RPCを呼び出し
                    print(f"--- Node {ip}:{port} departing (STOP) via RPC ---")
                    wait_until_down(ip, port, timeout=5.0)
                    DEPARTED_NODES[ip] = {"port": int(port), "mode": "DEPART"}
                    _record_churn_event("DEPART", ip, port)
                    before_depart(ip, port)
                except Exception as e:
                    print(f"ERROR: Failed to DEPART node {ip}:{port}. {e}")
                return True

            if action == 'CRASH':
                # CRASH: RPCは呼ばず、SIGKILLだけを実行
                ok = crash_remote_node(ip)
                if ok:
                    # すぐ死なない場合もあるので軽く待つ
                    wait_until_down(ip, port, timeout=5.0)
                    DEPARTED_NODES[ip] = {"port": int(port), "mode": "CRASH"}
                    _record_churn_event("CRASH", ip, port)
                    before_depart(ip, port)
                    return True
                else:
                    print(f"ERROR: Failed to CRASH node {ip}:{port}.")
                    return False
        return False

STOP_CHURN = threading.Event()

def churn_manager(all_servers, duration, window_sec=30.0, max_events_per_window=2, min_events_per_window=1, max_retries_per_event=5):
    """
    メインのput/get処理と並列で実行されるチャーン管理スレッド

    - window_sec 秒ごとに区切る
    - 各 window 内で最大 max_events_per_window 回まで churn を発生
    - 発生タイミングは window 内でランダム
    """
    start_ts = time.time()
    now = start_ts

    while (not STOP_CHURN.is_set()) and (now - start_ts < duration):
        window_start = now
        window_end = min(window_start + window_sec, start_ts + duration)

        # 今ウィンドウで発生させる churn 回数（min〜max）
        num_events = random.randint(min_events_per_window, max_events_per_window)

        # window 内のランダムな時刻に配置
        event_times = sorted(
            random.uniform(window_start, window_end)
            for _ in range(num_events)
        )

        for t_evt in event_times:
            if STOP_CHURN.is_set():
                break

            sleep_time = t_evt - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

            # 必ず churn を起こす：失敗したら同一window内でリトライ
            ok = False
            for _ in range(max_retries_per_event):
                ok = induce_churn_event(all_servers)
                if ok:
                    break
                # 少し待って再試行（リングが不安定・RPC復帰直後などを避ける）
                time.sleep(0.5)

        # 次の window まで待機
        now = window_end
        if not STOP_CHURN.is_set() and now > time.time():
            time.sleep(now - time.time())

def try_snapshot_counts(ip, port, timeout=2):
    st = NODE_STATS[(ip, port)]
    try:
        p = clientxmlrpc.ChordClientxmlrpcProxy(ip, port, timeout=timeout)
        snap = p.get_message_counts()
        now = time.time()
        st.mark_up(now)
        st.add_counts_snapshot(snap)
        st.last_seen_ok = now
        return True
    except Exception:
        # ここで即 DOWN にしない（瞬間的な失敗でぶれないように）
        return False

def try_snapshot_transfer_stats(ip, port, timeout=2):
    """CRASH 対応: 取れるときに transfer_stats を吸い上げて last-seen を更新する"""
    node_uid = str(get_uid(ip, port))
    try:
        p = clientxmlrpc.ChordClientxmlrpcProxy(ip, port, timeout=timeout)
        st = p.get_transfer_stats()
        if isinstance(st, dict):
            now = time.time()
            LAST_SEEN_TRANSFER_STATS[node_uid] = {"ts": now, "stats": dict(st)}
            return True
        return False
    except Exception:
        return False

def before_depart(ip, port):
    # 落とす直前にカウンタ回収（message_counts + transfer_stats）→ uptime 閉じる
    try_snapshot_counts(ip, port, timeout=2)
    NODE_STATS[(ip, port)].mark_down(time.time())

def after_rejoin(ip, port):
    NODE_STATS[(ip, port)].mark_up(time.time())
    # すぐは RPC が通らないことがあるので、少し待って初回スナップショットを入れる
    time.sleep(0.5)
    try_snapshot_counts(ip, port, timeout=2)
    try_snapshot_transfer_stats(ip, port, timeout=2)

STOP_SAMPLER = threading.Event()

def sampler_loop(all_servers, interval=3, down_grace=9):
    # down_grace: 何秒成功しなかったらDOWN扱いにするか
    while not STOP_SAMPLER.is_set():
        now = time.time()
        for ip, port in all_servers:
            ok = try_snapshot_counts(ip, port, timeout=2)
            try_snapshot_transfer_stats(ip, port, timeout=2)
            st = NODE_STATS[(ip, port)]
            if not ok:
                # 一定期間成功しないなら DOWN と判定して uptime を閉じる
                with st.lock:
                    last_ok = st.last_seen_ok
                    is_up = st.is_up
                if is_up and last_ok is not None and (now - last_ok) > down_grace:
                    st.mark_down(now)
        time.sleep(interval)

def normalize_host(s, default_port=17000):
    # s が ('192.168.4.100',17000) でも '192.168.4.100' でも動くようにする
    if isinstance(s, tuple) and len(s) == 2:
        return s[0], int(s[1])
    if isinstance(s, dict) and "ip" in s:
        return s["ip"], int(s.get("port", default_port))
    return str(s), int(default_port)


def pick_query_ip(all_servers_on):
    """
    put, get中に問い合わせ先のノードを1つに固定しない
    """
    with CHURN_LOCK:
        departed = set(DEPARTED_NODES.keys())
    candidates = [ip for (ip, port) in all_servers_on if ip not in departed]
    if not candidates:
        candidates = [ip for (ip, port) in all_servers_on]
    return random.choice(candidates)

def process_one_put(c, all_servers_on):
    """
    1件のcontentをPUTするworker。
    ThreadPoolExecutorから呼ばれる前提。
    戻り値:
      {
        "ok": bool,
        "content": c,
        "query_ip": str,
        "res": RPC戻り値 or None,
        "err_kind": str or None,
        "err_msg": str
      }
    """
    query_ip = None

    try:
        query_ip = pick_query_ip(all_servers_on)

        # 重要:
        # 並列時は get_proxy() のキャッシュを使わず、各workerで新規proxyを作る
        proxy = ChordClientxmlrpcProxy(
            query_ip,
            17000,
            timeout=DEFAULT_CONNECTION_TIMEOUT
        )

        res = call_with_retries(
            proxy.put_content,
            c,
            retries=RPC_RETRIES_CLIENT,
            backoff=RPC_BACKOFF_CLIENT,
            retry_on_none=False,
        )

        ok = (
            isinstance(res, dict)
            and res.get("ok") is True
            and res.get("uid") == c.get("uid")
        )

        if ok:
            return {
                "ok": True,
                "content": c,
                "query_ip": query_ip,
                "res": res,
                "err_kind": None,
                "err_msg": "",
            }

        return {
            "ok": False,
            "content": c,
            "query_ip": query_ip,
            "res": res,
            "err_kind": "PUT_FAIL_RET",
            "err_msg": f"uid={c.get('uid', '')[:8]} query_ip={query_ip} res={type(res).__name__}:{repr(res)[:200]}",
        }

    except Exception as e:
        return {
            "ok": False,
            "content": c,
            "query_ip": query_ip,
            "res": None,
            "err_kind": "PUT_FAIL_EXC",
            "err_msg": f"uid={c.get('uid', '')[:8]} query_ip={query_ip} {type(e).__name__}: {e}",
        }

def _get_hit_uid_from_response(res):
    """
    chord.py の get_content() の戻り値を正規化する。

    元Chordでは:
      hit  -> list[dict]
      miss -> [hop, rHop] または tuple(hop, rHop)
      error/timeout -> None
    になりうる。
    """
    if res is None:
        return None, "GET_RAW_NONE"

    # miss: XML-RPC経由だと tuple が list になることがある
    if isinstance(res, (list, tuple)) and len(res) == 2 and all(isinstance(x, int) for x in res):
        return None, "GET_MISS_HOP"

    # hit: list[dict]
    if isinstance(res, list) and res and isinstance(res[0], dict):
        return str(res[0].get("uid", "")), None

    # 念のため: dict単体で返る版にも対応
    if isinstance(res, dict):
        return str(res.get("uid", "")), None

    return None, f"GET_BAD_TYPE_{type(res).__name__}"


def process_one_get(c, all_servers_on):
    """
    1件のcontentをGETするworker。
    """
    query_ip = None

    try:
        query_ip = pick_query_ip(all_servers_on)

        # 並列時はproxy共有しない
        proxy = ChordClientxmlrpcProxy(
            query_ip,
            17000,
            timeout=DEFAULT_CONNECTION_TIMEOUT
        )

        res = call_with_retries(
            proxy.get_content,
            c["uid"],
            retries=RPC_RETRIES_CLIENT,
            backoff=RPC_BACKOFF_CLIENT,
            retry_on_none=False,
        )

        got_uid, err_kind = _get_hit_uid_from_response(res)

        if got_uid is not None and got_uid == c["uid"]:
            return {
                "ok": True,
                "content": c,
                "query_ip": query_ip,
                "res": res,
                "err_kind": None,
                "err_msg": "",
            }

        if got_uid is not None and got_uid != c["uid"]:
            return {
                "ok": False,
                "content": c,
                "query_ip": query_ip,
                "res": res,
                "err_kind": "GET_UID_MISMATCH",
                "err_msg": f"query_ip={query_ip} req={c['uid'][:8]} got={got_uid[:8]}",
            }

        return {
            "ok": False,
            "content": c,
            "query_ip": query_ip,
            "res": res,
            "err_kind": err_kind or "GET_FAIL_UNKNOWN",
            "err_msg": f"query_ip={query_ip} uid={c['uid'][:8]} res={type(res).__name__}:{repr(res)[:200]}",
        }

    except Exception as e:
        return {
            "ok": False,
            "content": c,
            "query_ip": query_ip,
            "res": None,
            "err_kind": "GET_FAIL_EXC",
            "err_msg": f"query_ip={query_ip} uid={c.get('uid', '')[:8]} {type(e).__name__}: {e}",
        }


if __name__ == '__main__':
    start_time = time.time()
    EXPERIMENT_T0 = start_time
    print(f"[INFO] Experiment started at {time.ctime(start_time)}")

    #自分の情報(使わない)
    ip='131.113.135.243'
    port=17000
    
    #bootstrap
    bootIp='172.16.1.11'
    #bootIp='192.168.0.160'
    bootPort=17000
    bootUid=get_uid(bootIp,bootPort)
    
    #全てのサーバの情報(メタデータ。通常時は使用不可)
    #numlist=[82,83,84]
    
    servers=[('172.16.1.11',17000), ('172.16.2.11',17000), ('172.16.3.11',17000), ('172.16.2.12',17000),
            ('172.16.1.12',17000), ('172.16.3.12',17000), ('172.16.4.11',17000), ('172.16.4.12',17000), 
            ('172.16.8.11',17000), ('172.16.5.11',17000),
            ] #全サーバ情報

    all_servers_on = servers

    for ip, port in all_servers_on:
        NODE_STATS[(ip, port)] = NodeStats(ip, port)

    # 実験開始前のメッセージカウントをリセット
    for ip, port in all_servers_on:
        try:
            proxy = clientxmlrpc.ChordClientxmlrpcProxy(ip, port)
            proxy.reset_message_counts()
            proxy.reset_exploration_hops()
        except Exception as e:
            log.warning(f"Failed to reset message counts on server {ip}:{port}: {e}")
    
    #RPCクライアント。bootstrapノードのNodeInterfaceを作る
    bootNode=chord.BasicNode(bootIp,bootPort)
    bootNodeIF=chord.NodeInterface(bootNode.asdict())
    
    #初期値設定
    repeat=10 #何回実験を行うか=EFTの平均長
    times=[] #実行時間
    random.seed(58)
    
    #グラフ設定
    N_NODES = 10000
    M_EDGES = 5

    # import networkx as nx
    # g = nx.barabasi_albert_graph(N_NODES, M_EDGES)
    #fd='/Users/sakuraeguchi/Desktop/research/code/data/'
    #fd='/home/yuushi/expdata/data/'
    #fn='facebook_combined.txt'
    #fn='fb-pages-food.txt'
    #fn='twitter_combined.txt'
    

    #実行前にdhtから全部のコンテンツを消す
    bootNodeIF.methodProxy.delete_all_contents_servers()

    # メッセージカウンタをリセット
    try:
        bootNodeIF.methodProxy.reset_message_counts()
        print(f"Message counts reset")
    except Exception as e:
        print(f"Error resetting message counts: {e}")

    # GET/PUTが実行される総時間の見込み (実験時間に応じて調整)
    # put_contentとget_contentの総実行時間を基に、チャーンスレッドの生存時間を設定
    TOTAL_EXPERIMENT_DURATION = 2000
    DEBUG_NONE_SAMPLE_LIMIT = 200  # diagnostics only
    # 例: 60秒間チャーンを継続

    # チャーン管理スレッドの起動
    ENABLE_CHURN = True

    if ENABLE_CHURN:
        churn_thread = threading.Thread(
            target=churn_manager,
            args=(all_servers_on, TOTAL_EXPERIMENT_DURATION),
            kwargs={"window_sec": 150.0, "max_events_per_window": 2, "min_events_per_window": 1}
        )

        churn_thread.start()

    sampler_thread = threading.Thread(target=sampler_loop, args=(all_servers_on,))
    sampler_thread.start()

    contents = generate_synthetic_contents(num_items=100000, gid="synthetic")

    #DHTに全ノードを登録
    print('putting all contents')
    put_ok_contents = []

    with ThreadPoolExecutor(max_workers=MAX_PUT_WORKERS) as executor:
        future_to_content = {
            executor.submit(process_one_put, c, all_servers_on): c
            for c in contents
        }

        for fut in as_completed(future_to_content):
            c = future_to_content[fut]

            bucket = _time_bucket_index()
            departed_cnt = current_departed_count()

            put_attempts += 1
            TIME_BUCKET_STATS[bucket]["put_attempts"] += 1
            CHURN_BUCKET_STATS[departed_cnt]["put_attempts"] += 1

            try:
                r = fut.result()
            except Exception as e:
                stats["put_fail"] += 1
                TIME_BUCKET_STATS[bucket]["put_fail"] += 1
                CHURN_BUCKET_STATS[departed_cnt]["put_fail"] += 1
                record_err("PUT_WORKER_EXC", f"uid={c.get('uid', '')[:8]} {type(e).__name__}: {e}")
                continue

            if r.get("ok") is True:
                stats["put_ok"] += 1
                TIME_BUCKET_STATS[bucket]["put_ok"] += 1
                CHURN_BUCKET_STATS[departed_cnt]["put_ok"] += 1
                put_ok_contents.append(c)
            else:
                stats["put_fail"] += 1
                TIME_BUCKET_STATS[bucket]["put_fail"] += 1
                CHURN_BUCKET_STATS[departed_cnt]["put_fail"] += 1
                record_err(r.get("err_kind") or "PUT_FAIL_UNKNOWN", r.get("err_msg", ""))

    print(f"[PUT_DONE] ok={stats['put_ok']} fail={stats['put_fail']} attempts={put_attempts}")


    '''①順番にノードを問い合わせるときにかかる時間を計測'''
    print('one by one')
    print(datetime.datetime.now())
    startTime=time.time()
    cntIds=set() #DHTで集めたノード情報. IDのリスト
    hopsEach=[]


    # GETはPUT成功したものだけに限定する
    get_targets = put_ok_contents

    with ThreadPoolExecutor(max_workers=MAX_GET_WORKERS) as executor:
        future_to_content = {
            executor.submit(process_one_get, c, all_servers_on): c
            for c in get_targets
        }

        for fut in as_completed(future_to_content):
            c = future_to_content[fut]

            bucket = _time_bucket_index()
            departed_cnt = current_departed_count()

            get_attempts += 1
            TIME_BUCKET_STATS[bucket]["get_attempts"] += 1
            CHURN_BUCKET_STATS[departed_cnt]["get_attempts"] += 1

            try:
                r = fut.result()
            except Exception as e:
                stats["get_fail"] += 1
                TIME_BUCKET_STATS[bucket]["get_fail"] += 1
                CHURN_BUCKET_STATS[departed_cnt]["get_fail"] += 1
                record_err("GET_WORKER_EXC", f"uid={c.get('uid', '')[:8]} {type(e).__name__}: {e}")
                continue

            if r.get("ok") is True:
                stats["get_ok"] += 1
                TIME_BUCKET_STATS[bucket]["get_ok"] += 1
                CHURN_BUCKET_STATS[departed_cnt]["get_ok"] += 1

                # 元コードの集計に合わせて、idも記録しておく
                try:
                    cntIds.add(c["id"])
                except Exception:
                    pass

                # hop集計したい場合
                res = r.get("res")
                if isinstance(res, list) and res and isinstance(res[0], dict):
                    try:
                        hopsEach.append(res[0].get("hop", 0))
                    except Exception:
                        pass
                elif isinstance(res, dict):
                    try:
                        hopsEach.append(res.get("hop", 0))
                    except Exception:
                        pass

            else:
                stats["get_fail"] += 1
                TIME_BUCKET_STATS[bucket]["get_fail"] += 1
                CHURN_BUCKET_STATS[departed_cnt]["get_fail"] += 1
                record_err(r.get("err_kind") or "GET_FAIL_UNKNOWN", r.get("err_msg", ""))

        timeByEach=time.time()-startTime

    STOP_CHURN.set()
    churn_thread.join(timeout=10)

    STOP_SAMPLER.set()
    sampler_thread.join(timeout=10)

    end_time = time.time()
    for st in NODE_STATS.values():
        st.finalize(end_time)

    
    # 測定結果の取得と表示
    print("Collecting message counts from all servers...")
    total_messages = 0
    total_exploration_hops = 0
    aggregate_counts = {}  # 全体の集計用
    total_uptime = 0.0

    for (ip, port), st in sorted(NODE_STATS.items()):
        with st.lock:
            uptime = st.uptime_total
            counts = dict(st.counts_total)
        
        total_uptime += uptime
        
        print(f"--- {ip}:{port} ---")
        for k, v in counts.items():
            if v > 0:
                print(f"  {k}: {v}")
                aggregate_counts[k] = aggregate_counts.get(k, 0) + v
            total_messages += sum(counts.values())


    print("\n=== Aggregated results across all servers ===")
    for k, v in aggregate_counts.items():
        if v > 0:
            print(f"{k}: {v}")
    print(f"Total: {total_messages}")

    print("\n=== Client put/get results ===")
    print(f"put_ok: {stats.get('put_ok', 0)}")
    print(f"put_fail: {stats.get('put_fail', 0)}")
    print(f"get_ok: {stats.get('get_ok', 0)}")
    print(f"get_fail: {stats.get('get_fail', 0)}")
    print(f"put_attempts: {put_attempts}")
    print(f"get_attempts: {get_attempts}")

    
    print(f"[INFO] Experiment finished at {time.ctime(end_time)}")

    elapsed = end_time - start_time
    print(f"[RESULT] Total elapsed time: {elapsed:.2f} seconds")

    print("\n=== Results by number of departed nodes ===")
    for k in sorted(CHURN_BUCKET_STATS.keys()):
        b = CHURN_BUCKET_STATS[k]
        print(f"\n[Departed nodes = {k}]")
        print(f" PUT: {b['put_ok']} ok / {b['put_fail']} fail "
            f"(attempts={b['put_attempts']})")
        print(f" GET: {b['get_ok']} ok / {b['get_fail']} fail "
            f"(attempts={b['get_attempts']})")


    print("\n=== Churn events timeline (seconds since start) ===")
    if not CHURN_EVENTS:
        print("(no churn events recorded)")
    else:
        for ev in CHURN_EVENTS:
            # 例: t=123.4s DEPART 192.168.4.107:17000 departed_after=2
            print(f"t={ev['t']:.1f}s {ev['action']:6s} {ev['ip']}:{ev['port']} departed_after={ev['departed_after']}")

    print("\n=== Results by 60-second windows ===")
    elapsed = end_time - start_time
    full_windows = int(elapsed // BUCKET_SECONDS)  # 630秒なら10（0..9）。最後の余り30秒は出ない。
    for w in range(full_windows):
        b = TIME_BUCKET_STATS[w]
        start_s = w * BUCKET_SECONDS
        end_s = (w + 1) * BUCKET_SECONDS
        print(f"\n[Window {w}] {start_s}-{end_s} sec")
        if b["put_attempts"] > 0:
            print(f" PUT: {b['put_ok']} ok / {b['put_fail']} fail (attempts={b['put_attempts']})")
        if b["get_attempts"] > 0:
            print(f" GET: {b['get_ok']} ok / {b['get_fail']} fail (attempts={b['get_attempts']})")
        if b["put_attempts"] == 0 and b["get_attempts"] == 0:
            print(" (no put/get attempts)")

    print("\n=== Error breakdown (client-side) ===")
    for k in sorted(ERR_COUNTS.keys()):
        print(f"{k}: {ERR_COUNTS[k]}")

    print("\n=== Error samples (up to 10 each) ===")
    for k in sorted(ERR_SAMPLES.keys()):
        if ERR_SAMPLES[k]:
            print(f"\n[{k}]")
            for s in ERR_SAMPLES[k]:
                print(" ", s)
    
    print("\n=== Transfer stats (receiver-side) ===")
    agg_ts, per_node_ts, archive_seen = collect_transfer_stats(all_servers_on)

    # 見やすい順に並べる（attempt/applied/new/updated）
    keys_sorted = sorted(agg_ts.keys())
    for k in keys_sorted:
        print(f"{k}: {agg_ts[k]}")
    
    # ノード別（離脱/クラッシュ含む）
    print("\n--- Per-node (including departed/crashed) ---")
    # ip順で見やすく
    def _ip_sort_key(item):
        uid, info = item
        try:
            ip = info.get("ip", "")
            return [int(x) for x in ip.split(".")] + [int(info.get("port", 0))]
        except Exception:
            return [999, 999, 999, 999, 0]

    for node_uid, info in sorted(per_node_ts.items(), key=_ip_sort_key):
        ip = info.get("ip")
        port = info.get("port")
        src = info.get("source")
        ts = info.get("ts")
        if ts is None:
            ts_s = "n/a"
        else:
            # 実験開始時刻からの相対で出すと比較しやすい
            try:
                ts_s = f"{(ts - EXPERIMENT_T0):.1f}s"
            except Exception:
                ts_s = str(ts)
        print(f"[{ip}:{port}] uid={node_uid[:8]} source={src} ts={ts_s} items={len(info.get('stats') or {})}")


    # 重要なサマリ（reason別）
    def _get(k): return int(agg_ts.get(k, 0))
    reasons = ["handoff", "join_pull", "repair", "put_replica", "put_primary", "unknown"]
    print("\n--- Summary by reason ---")
    for r in reasons:
        a = _get(f"{r}_recv_attempt")
        ap = _get(f"{r}_recv_applied")
        nu = _get(f"{r}_recv_new")
        up = _get(f"{r}_recv_updated")
        if a or ap or nu or up:
            print(f"{r}: attempt={a} applied={ap} new={nu} updated={up}")

