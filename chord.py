''' originated with https://github.com/Titotix/pychord
各クラスがある場所：
chord.py
・BasicNode
・NodeInterface
・Finger
・LocalNode

serverxmlrpc.py
・RequestHandler
・ChordServerxmlrpc

key.py
・Key
・Uid

stabilizer.py
・Stabilizer

clientxmlrpc.py
・ChordClientxmlrpcProxy


call_with_retriesを通したget_contentができている状態
successor listを新しく用意し,データの複製を持たせることで
ノード障害(意図的でない離脱)にも対応できるようにしたい 
client_pointer2_ver9.pyとの組み合わせ

Chordでのルーティングがコメントアウト(find_successor部分など)されており実行していなかったため修正(20260610)
'''

import re
import os
import logging
import sys
import serverxmlrpc
import clientxmlrpc
import random
import concurrent.futures
import concurrent.futures.thread as cf_thread
import time
import xmlrpc.client
from stabilizer import Stabilizer
from collections import deque,defaultdict
from key import Key, Uid
from xmlrpc.client import Transport
from serverxmlrpc import MESSAGE_COUNTS, count_message, increment_exploration_hops, get_exploration_hops_snapshot, reset_exploration_hops_global
import atexit, signal, threading, traceback
from concurrent.futures import ThreadPoolExecutor
import queue

log = logging.getLogger('chordLog')
#log.setLevel(logging.INFO)
log.setLevel(logging.DEBUG)
if not any(isinstance(handler, logging.StreamHandler) for handler in log.handlers):
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(' %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)

# 設定（必要なら上書き）
RPC_PER_CALL_TIMEOUT = 0.3    # 各RPC呼び出しのタイムアウト（秒）
ROUTING_OVERALL_TIMEOUT = 2.0  # ルーティング全体のタイムアウト（秒）
RPC_RETRIES = 2               # 各呼び出しの再試行回数（短め）

# successor list / replication (fixed)
REPLICATION_FACTOR = 4  # successor list length and replica count (primary + 3 replicas)

# repair settings (best-effort)
REPAIR_MAX_KEYS_PER_STABILIZE = 200 # safety cap per stabilize tick

_RPC_SHUTTING_DOWN = False
_RPC_SHUTDOWN_SET_ONCE = False
_EXEC_TRACE_EMITTED = False

_ORIG_EXEC_SHUTDOWN = None
_EXEC_SHUTDOWN_LOGGED = False
_RPC_EXECUTOR_SHUTDOWN_ONCE = False

_RPC_EXECUTOR_LOCK = threading.Lock()

_ALREADY_SHUT_WARNED = False

def _ensure_rpc_executor():
    global _RPC_EXECUTOR
    # プロセス終了中なら復活させない
    if _RPC_SHUTTING_DOWN:
        return
    with _RPC_EXECUTOR_LOCK:
        try:
            if _RPC_EXECUTOR is None or getattr(_RPC_EXECUTOR, "_shutdown", False):
                _RPC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
                    max_workers=RPC_EXECUTOR_MAX_WORKERS
                )
                log.warning("[RPC_EXECUTOR_RECREATED] pid=%s thread=%s",
                            os.getpid(), threading.current_thread().name)
        except Exception as e:
            log.warning("[RPC_EXECUTOR_RECREATE_FAIL] %r", e)

def install_executor_shutdown_tracer(executor):
    global _ORIG_EXEC_SHUTDOWN
    if _ORIG_EXEC_SHUTDOWN is not None:
        return
    _ORIG_EXEC_SHUTDOWN = executor.shutdown

    def traced_shutdown(*args, **kwargs):
        global _EXEC_SHUTDOWN_LOGGED
        if not _EXEC_SHUTDOWN_LOGGED:
            _EXEC_SHUTDOWN_LOGGED = True
            log.warning("[EXECUTOR_SHUTDOWN_CALLED] module=%s pid=%s thread=%s exec_id=%s\n%s",
                        __name__, os.getpid(), threading.current_thread().name, id(executor),
                        "".join(traceback.format_stack(limit=40)))
        return _ORIG_EXEC_SHUTDOWN(*args, **kwargs)

    executor.shutdown = traced_shutdown

_EXEC_SHUTDOWN_LOGGED = False

def shutdown_rpc_executor(reason=""):
    global _EXEC_SHUTDOWN_LOGGED, _RPC_SHUTTING_DOWN
    _RPC_SHUTTING_DOWN = True

    # すでに shutdown 済みなら何もしない（多重呼び出し対策）
    if getattr(_RPC_EXECUTOR, "_shutdown", False):
        return

    if not _EXEC_SHUTDOWN_LOGGED:
        _EXEC_SHUTDOWN_LOGGED = True
        log.warning("[EXECUTOR_SHUTDOWN_REQUESTED] reason=%s pid=%s thread=%s\n%s",
                    reason, os.getpid(), threading.current_thread().name,
                    "".join(traceback.format_stack(limit=40)))

    try:
        _RPC_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    except Exception:
        pass


def _rpc_method_desc(method):
    # method が bound method とは限らないので __self__ を前提にしない
    self_obj = getattr(method, "__self__", None)
    ip = getattr(self_obj, "ip", "?")
    port = getattr(self_obj, "port", "?")
    name = getattr(method, "__name__", None)
    if not name:
        # xmlrpc._Method や partial 等
        name = getattr(getattr(method, "__class__", None), "__name__", "callable")
    return f"{ip}:{port}.{name}"

def set_rpc_shutting_down(reason: str):
    global _RPC_SHUTTING_DOWN, _RPC_SHUTDOWN_SET_ONCE
    if _RPC_SHUTDOWN_SET_ONCE:
        _RPC_SHUTTING_DOWN = True
        return
    _RPC_SHUTDOWN_SET_ONCE = True
    _RPC_SHUTTING_DOWN = True
    log.warning(
        "[SHUTDOWN_FLAG_SET] reason=%s pid=%s thread=%s\n%s",
        reason, os.getpid(), threading.current_thread().name,
        "".join(traceback.format_stack(limit=40)),
    )

def _thread_excepthook(args):
    log.error("[THREAD_EXC] in thread=%s: %s: %s",
                getattr(args.thread, "name", "?"),
                args.exc_type.__name__, args.exc_type)
    log.error("[THREAD_EXC] traceback:\n%s", "".join(traceback.format_tb(args.exc_traceback)))

threading.excepthook = _thread_excepthook

# main thread の未捕捉例外も取る
def _sys_excepthook(exctype, value, tb):
    log.error("[SYS_EXC] %s: %s\n%s", exctype.__name__, value, "".join(traceback.format_tb(tb)))
    set_rpc_shutting_down("sys.excepthook")
    shutdown_rpc_executor("sys.excepthook")
    # 既定の excepthook も呼ぶ
    sys.__excepthook__(exctype, value, tb)

sys.excepthook = _sys_excepthook

# executor はモジュール単位で共有（サーバプロセス内でスレッドによるタイムアウトを実現するため）
_RPC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=8)

log.warning("[EXECUTOR_CREATED]module=%s file=%s pid=%s exec_id=%s",
            __name__, __file__, os.getpid(), id(_RPC_EXECUTOR))

install_executor_shutdown_tracer(_RPC_EXECUTOR)


class BasicNode:
    """
    サーバの基礎クラス。他のクラスに継承してもらう想定。dictに余分なキーが入っててもOK
    Attrs:
    ・ip(str)
    ・port(int)
    ・uid(Uid)
    
    {"ip":<ip>, "port": <port>}
    """
    def __init__(self, *args): #引数の数が２パターンある。継承とは無関係
        # IPアドレスとポートを別々に渡す
        # BasicNode("127.0.0.1", 8000)
        if len(args) == 2:
            ip = args[0]
            port = args[1]
        # IPアドレスとポートを辞書で渡す
        # BasicNode({"ip": "127.0.0.1", "port": 8000})
        elif len(args) == 1:
            ip = args[0]["ip"]
            port = args[0]["port"]
        else:
            raise ValueError("len args of {} unsupported".format(len(args)))
        self.ip = ip
        self.port = port
        #TODO:optimization with sys.intern() str of 64 char
        #sys.intern() を使うと、同じ文字列をメモリ上で一つにまとめられるため、UIDの比較が高速になる。
        #「UIDが64文字固定なら、intern()して高速化できるかも」というメモ。
        #DHTにおけるキー。サーバのID。(IPアドレス:ポート番号)をハッシュ化。
        self.uid = Uid(self.ip + ":" + repr(self.port))

    def return_Uid(self):
        return self.uid

    def asdict(self):
        """
        RPC通信で使うために辞書形式でサーバの情報を渡す。ip,port,uid
        Creates and returns a dict with attr of the instance
        The dict can be used in rpc args
        """
        return {"ip": self.ip,
                "port": self.port,
                "uid": self.uid.value}

class NodeInterface(BasicNode):
    """
    BasicNodeを継承。各サーバをコピーして仮想サーバを作り、仮想サーバ経由でメソッドを実行することで、
    他サーバのメソッド使用時にいちいち通信用コードを書く必要がなくなる。
    """
    def __init__(self, arg):
        # 仮想ノードの作成
        if isinstance(arg, LocalNode):  # 自サーバならそのまま実行
            super().__init__(arg.ip, arg.port)  # 該当サーバの情報をコピー
            self.methodProxy = arg  # ローカルノードは直接呼べる
        elif isinstance(arg, dict):  # 他サーバならRPC
            super().__init__(arg)
            try:
                # clientxmlrpc.create_proxy() を使用して安全なRPCプロキシを作成
                self.methodProxy = clientxmlrpc.create_proxy(
                    self.ip, int(self.port), timeout=3
                )
            except Exception as e:
                log.warning(f"Failed to create RPC proxy for {self.ip}:{self.port}: {e}")

                # フォールバック: 失敗したらダミープロキシを入れてNoneを返すようにする
                class _DummyProxy:
                    def __getattr__(self, name):
                        def _dummy(*args, **kwargs):
                            raise ConnectionError(f"RPC proxy unavailable for {arg.get('ip')}:{arg.get('port')} method={name}")
                        return _dummy
                self.methodProxy = _DummyProxy()
        else:
            raise TypeError("Supports LocalNode or dict")


class LocalNode(BasicNode):
    """各サーバを表すクラス。このクラスのインスタンスを作成するとRPC用のサーバも立ち上がる

    Attrs:
        BasicNode (_type_): _description_
        ip
        port
        uid(Uid):ハッシュ値
        predecessor：自分の前にいるノードのNodeInterface型
        fingers (list): [Finger, Finger...]
        exFingers (dict): {respNode.uid:ExFinger,...}GFT．GFTのみを使用してルーティングする際はexFingers[0]にsuccessorを入れること
        server(ChordServerxmlrpc): RPC用のTCPサーバを定義
        _stabilizer(Stabilizer):スタビライザ。ストップされるまで一定時間おきにスタビライズ処理をする
        contents(dict): 実際のコンテンツを入れる。{key(str):[data(Contents),data,...]}　ちなみに各dataにContents.uid.value=keyが含まれてるので冗長
        searched(set):探索されたkey(str)のリスト。

    Raises:
        TypeError: _description_
        TypeError: _description_
        ValueError: _description_

    Returns:
        _type_: _description_
    """
    def __init__(self, ip:str, port:str, _stabilizer=True):
        BasicNode.__init__(self, ip, port) #自身のIPアドレスとポートを設定
        self.predecessor = None
        self.fingers = []
        self.exFingers = {} #GFTの初期化は外部からの指示でやる
        self.searched=set()
        #FT作成。
        self.create_finger_table()       
        # --- storage/state (must exist before any background activity) ---
        self.contents = {}
        self.searched = set()
        self.msgcounts = {}
        self._stopping = False
        self._stop_event = threading.Event()

        self.transfer_stats = defaultdict(int)
        self._transfer_lock = threading.Lock()

        self._repl_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"repl-{self.ip}")

        self.departed_stats_archive = {}
        self._departed_lock = threading.Lock()

        # --- successor list / replication state ---
        # successor_list[0] should be this node's successor (NodeInterface)
        self.successor_list = []
        self._succ_list_lock = threading.Lock()
        # version: PUT counter per key (monotonic)
        self._put_counter_by_key = defaultdict(int)

        self._rpc_server = serverxmlrpc.ChordServerxmlrpc(ip, int(port), self)

        if hasattr(self._rpc_server, "start") and callable(getattr(self._rpc_server, "start")):
            # Thread派生など
            self._rpc_server.start()
        elif hasattr(self._rpc_server, "serve_forever") and callable(getattr(self._rpc_server, "serve_forever")):
            # SimpleXMLRPCServer など（startが無い）
            self._rpc_thread = threading.Thread(
                target=self._rpc_server.serve_forever,
                name=f"RPC-{ip}:{port}",
                daemon=True,
            )
            self._rpc_thread.start()
        else:
            raise RuntimeError(f"ChordServerxmlrpc has no start/serve_forever: {type(self._rpc_server)}")

        self.server = self._rpc_server 


        self._stabilizer = _stabilizer
        if _stabilizer:
            self.stabilizer = Stabilizer(self)
            self.stabilizer.start()

        # initialize successor list (best-effort)
        try:
            self._refresh_successor_list()
        except Exception:
            # at bootstrap (single node), successor may be self
            try:
                with self._succ_list_lock:
                    self.successor_list = [self.successor] if getattr(self, 'fingers', None) else []
            except Exception:
                pass


    @property
    def successor(self):
        '''sucだけ返しやすく。nodeinterface型で返る。'''
        return self.fingers[0].respNode

    # ----------------------------
    # successor list (RPC-visible)
    # ----------------------------
    def get_successor_list(self):
        """Return this node's successor list for RPC (primitives only).

        Returns:
            list[dict]: each dict has ip/port/uid
        """
        out = []
        try:
            with self._succ_list_lock:
                lst = list(self.successor_list)
        except Exception:
            lst = []

        for n in lst[:REPLICATION_FACTOR]:
            try:
                out.append({"ip": n.ip, "port": int(n.port), "uid": n.uid.value})
            except Exception:
                continue
        return out

    def ping(self):
        """
        Lightweight liveness check.
        - successor list maintenance などが「生存確認」に使う
        - 副作用なし・高速で返す
        XML-RPC の戻り値はプリミティブ型が無難なので True を返す。
        """
        # stopping/closing でも「応答できるうちは応答する」方針なら True のままでOK。
        # もし stopping 中は生存扱いにしたくないなら False を返す設計もあり得る。
        return True

    def _is_node_alive(self, node_if: "NodeInterface") -> bool:
        """Best-effort liveness check used for successor list maintenance."""
        try:
            # ping is cheap if exists; otherwise fall back to get_successor
            mp = getattr(node_if, "methodProxy", None)
            if mp is None:
                return False
            if hasattr(mp, "ping"):
                ret = mp.ping()
                # ping は True を返した場合のみ alive
                return ret is True
            else:
                # get_successor が dict を返せたら alive
                ret = mp.get_successor()
                return isinstance(ret, dict) and ret.get("ip") is not None
        except Exception:
            return False

    def _refresh_successor_list(self):
        """
        Refresh successor list by asking current successor for its list.
        Rule of thumb (Chord):
          successor_list = [successor] + successor.successor_list (trim to r)
        """
        try:
            succ = self.successor
        except Exception:
            return

        # Candidate pool:
        #   current successor + old successor_list + some fingers
        new_list = []
        try:
            if succ and succ != self:
                new_list.append(succ)
        except Exception:
            pass

        # 旧 successor_list を混ぜる（successor が死んでても再構築できるように）
        try:
            with self._succ_list_lock:
                old_list = list(getattr(self, "successor_list", []) or [])
        except Exception:
            old_list = list(getattr(self, "successor_list", []) or [])
        for n in old_list:
            try:
                if n and n != self:
                    new_list.append(n)
            except Exception:
                continue

        # finger の候補も少し混ぜる
        try:
            for f in getattr(self, "fingers", [])[:]:
                rn = getattr(f, "respNode", None)
                if rn and rn != self:
                    new_list.append(rn)
        except Exception:
            pass

        # Pull list from successor
        pulled = []
        try:
            # successor が alive のときだけ pull（死んでると recovery を阻害する）
            if succ and succ != self and self._is_node_alive(succ):
                pulled = succ.methodProxy.get_successor_list()
        except Exception:
            pulled = []

        # Normalize pulled entries into NodeInterface
        for d in pulled or []:
            try:
                ni, _ = self.get_NodeInterface(d)
                if ni and ni.uid.value != self.uid.value:
                    new_list.append(ni)
            except Exception:
                continue

        # De-dup + liveness filter, keep up to r
        dedup = []
        seen = set()
        for n in new_list:
            try:
                uidv = n.uid.value
            except Exception:
                continue
            if uidv in seen:
                continue
            seen.add(uidv)
            # remove dead nodes
            if self._is_node_alive(n):
                dedup.append(n)
            if len(dedup) >= REPLICATION_FACTOR:
                break

        try:
            with self._succ_list_lock:
                self.successor_list = dedup
        except Exception:
            pass

    # ----------------------------
    # replication RPCs (best-effort, async)
    # ----------------------------
    def replicate_put(self, content: dict):
        """Replica-side store. Does NOT trigger further replication."""
        try:
            # mark as replica store (optional; store_content currently ignores)
            content = dict(content)
            content["_replica"] = True
        except Exception:
            pass
        return self.store_content(content)
    
    def _replicate_async(self, content: dict):
        """
        Best-effort async replication to successor_list[0:REPLICATION_FACTOR-1]
        - never raises
        - never blocks primary ACK
        - NO routing fallback (do not call get_successor)
        """
        if not isinstance(content, dict):
            return

        # lazy init: small local executor for replication only
        if not hasattr(self, "_repl_exec"):
            self._repl_exec = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="repl")

        # snapshot successor_list
        try:
            with self._succ_list_lock:
                slist = list(getattr(self, "successor_list", []) or [])
        except Exception:
            slist = list(getattr(self, "successor_list", []) or [])

        # primary 自身以外へ。最大 REPLICATION_FACTOR-1 個
        targets = []
        for n in slist[:max(0, REPLICATION_FACTOR - 1)]:
            try:
                ip = getattr(n, "ip", None)
                port = int(getattr(n, "port", 0))
                uidv = getattr(getattr(n, "uid", None), "value", None)
                if ip and port and uidv and uidv != self.uid.value:
                    targets.append((ip, port, uidv))
            except Exception:
                continue

        if not targets:
            return

        def _one(ip, port, uidv):
            try:
                # replication は短い timeout でいい（best-effort）
                proxy = clientxmlrpc.create_proxy(ip, port, timeout=1.0)
                # replica も store_content でOK（あなたの回答通り）
                proxy.store_content(content)
            except Exception:
                return False
            return True

        for ip, port, uidv in targets:
            try:
                self._repl_exec.submit(_one, ip, port, uidv)
            except Exception:
                pass

    def _replicate_async_best_effort(self, content_with_ver: dict):
        """
        successor_list への best-effort 複製（put_replica）
        失敗しても例外で止めない
        """
        try:
            # candidates: successor_list (NodeInterface)
            try:
                with self._succ_list_lock:
                    sl = list(self.successor_list or [])
            except Exception:
                sl = list(getattr(self, "successor_list", []) or [])

            if not sl:
                return

            # meta for replicas
            base = dict(content_with_ver)
            try:
                m = dict(base.get("__meta") or {})
                m["reason"] = "put_replica"
                m.setdefault("src", f"{self.ip}:{int(self.port)}")
                base["__meta"] = m
            except Exception:
                pass

            # fire-and-forget threads (or use your existing executor/threading)
            for n in sl[:REPLICATION_FACTOR]:
                try:
                    # skip self
                    if getattr(n, "uid", None) and n.uid == self.uid:
                        continue
                    # best-effort: short timeout
                    n.methodProxy.store_content(base)  # receiver will count put_replica_recv_*
                except Exception:
                    continue
        except Exception:
            return


    def _replicate_one(self, node_if, content_with_ver: dict):
        """
        1台へ複製。失敗はログ最小で握りつぶす。
        """
        try:
            # 重要: fallback で successor を辿らない（遅くなるだけ）
            # replica は store_content を使ってOK（ユーザ要件）
            self._safe_rpc_call(node_if.methodProxy.store_content, content_with_ver, timeout=RPC_PER_CALL_TIMEOUT)
        except Exception:
            pass
    
    def get_transfer_stats(self):
        """Return transfer stats (primitive dict) for client aggregation."""
        try:
            with self._transfer_lock:
                return dict(self.transfer_stats)
        except Exception:
            return {}

    def store_content_primary(self, content: dict):
        """
        primary保存: ここでは put_primary として meta を付与して store_content する
        その後、非同期で put_replica を投げる（best-effort）
        """
        # attach meta (compat)
        try:
            c2 = dict(content)
            m = dict(c2.get("__meta") or {})
            m.setdefault("reason", "put_primary")
            m.setdefault("src", f"{self.ip}:{int(self.port)}")
            c2["__meta"] = m
        except Exception:
            c2 = content

        ack = self.store_content(c2)  # local store (counts receiver stats)
        # replicate only if store ok
        try:
            if isinstance(ack, dict) and ack.get("ok") is True:
                # make sure version exists & monotonic
                try:
                    v = int(c2.get("ver") or ack.get("ver") or 0)
                except Exception:
                    v = 0
                c2["ver"] = v
                self._replicate_async_best_effort(c2)  # best-effort replication
        except Exception:
            pass
        return ack

    
    def _asdict_min(self):
        """
        Minimal node dict for repair RPCs (primitives only).
        """
        try:
            return {"ip": self.ip, "port": int(self.port), "uid": self.uid.value}
        except Exception:
            return {"ip": getattr(self, "ip", None),
                    "port": int(getattr(self, "port", 0) or 0),
                    "uid": str(getattr(getattr(self, "uid", None), "value", ""))}
    
    def _best_local_content_with_ver(self, key: str):
        """
        Pick the locally stored content with the maximum version for a key.
        Returns: (best_dict, best_ver) or (None, 0)
        """
        best = None
        best_ver = 0
        try:
            clist = list(getattr(self, "contents", {}).get(key, []) or [])
        except Exception:
            clist = []

        for c in clist:
            try:
                v = int(getattr(c, "ver", 0) or 0)
            except Exception:
                v = 0
            if best is None or v > best_ver:
                best_ver = valid
                try:
                    d = c.asdict() if hasattr(c, "asdict") else (c if isinstance(c, dict) else dict(getattr(c, "__dict__", {})))
                except Exception:
                    d = {}
                try:
                    if isinstance(d, dict):
                        d = dict(d)
                        d["ver"] = int(best_ver)
                except Exception:
                    pass
                best = d
        return best, int(best_ver)

    def fetch_content_with_ver(self, key: str):
        """
        repair専用RPC（primary基準）:
        このノードが持つ key の content のうち ver 最大のものを 1件だけ返す。
        返り値は primitives only の dict:
          {"ok": True/False, "uid": key, "ver": int, "content": dict or None}
        """
        count_message('fetch_content_with_ver')
        try:
            lst = list(getattr(self, "contents", {}).get(key, []) or [])
        except Exception:
            lst = []

        best = None
        best_ver = -1
        for c in lst:
            try:
                v = int(getattr(c, "ver", 0) or 0)
            except Exception:
                v = 0
            if v > best_ver:
                best_ver = v
                best = c

        if best is None:
            return {"ok": False, "uid": key, "ver": 0, "content": None}

        try:
            d = best.asdict()
        except Exception:
            d = {}
        try:
            if isinstance(d, dict):
                d = dict(d)
                d["ver"] = int(best_ver)
        except Exception:
            pass
        return {"ok": True, "uid": key, "ver": int(best_ver), "content": d}
    
    def replica_get_version(self, key: str):
        """
        Replica-side: return local max version for key (0 if missing).
        """
        count_message('replica_get_version')
        _, v = self._best_local_content_with_ver(key)
        return int(v)
    
    def repair_pull_from_primary(self, primary_dict, key_hex, expected_ver, event_id=None):
        """
        replica側: primaryから不足分をpullして保存する
        """
        try:
            # primary の NodeInterface を作る
            prim_if, _ = self.get_NodeInterface(primary_dict)

            # primary から key の内容を取得（あなたの実装に合わせて呼び出し）
            # 例: return_contents(key, hop, rHop)
            resp = prim_if.methodProxy.return_contents(key_hex, 0, 0)

            if not isinstance(resp, list) or not resp:
                return 0

            applied = 0
            for cdict in resp:
                if not isinstance(cdict, dict):
                    continue

                # ver チェック（念のため）
                try:
                    v = int(cdict.get("ver") or 0)
                except Exception:
                    v = 0
                if v < int(expected_ver):
                    # primaryが古いものを返した等はスキップ
                    continue

                # ★ meta を付与して store_content（受信側で repair_recv_* が積まれる）
                try:
                    m = dict(cdict.get("__meta") or {})
                    m["reason"] = "repair"
                    m["src"] = f"{primary_dict.get('ip')}:{int(primary_dict.get('port'))}"
                    m["event"] = event_id
                    cdict["__meta"] = m
                except Exception:
                    pass

                ack = self.store_content(cdict)
                if isinstance(ack, dict) and ack.get("ok") and ack.get("applied"):
                    applied += 1

            return applied
        except Exception:
            return 0


    def store_departed_transfer_stats(self, departed_uid, stats, ts):
        try:
            with self._departed_lock:
                self.departed_stats_archive[departed_uid] = {
                    "ts": ts,
                    "stats": dict(stats)
                }
            return True
        except Exception:
            return False


    def get_departed_transfer_stats_snapshot(self):
        try:
            with self._departed_lock:
                return dict(self.departed_stats_archive)
        except Exception:
            return {}

    def get_departed_transfer_stats_snapshot(self):
        """
        RPC: Return snapshot of departed nodes' transfer stats.
        Returned value must be XML-RPC safe (dict of dict).
        """
        try:
            with self._departed_lock:
                # deep-ish copy to avoid mutation during RPC
                snap = {}
                for uid, payload in self.departed_stats_archive.items():
                    if not isinstance(payload, dict):
                        continue
                    ts = payload.get("ts")
                    stats = payload.get("stats", {})
                    if not isinstance(stats, dict):
                        continue
                    snap[str(uid)] = {
                        "ts": ts,
                        "stats": dict(stats),
                    }
                return snap
        except Exception:
            return {}

    def _ni_to_dict(self, n) -> dict:
        """NodeInterface/BasicNode を set_successor() 用の dict に正規化"""
        try:
            if isinstance(n, dict):
                return {"ip": n["ip"], "port": n["port"], "uid": n.get("uid")}
            return {"ip": n.ip, "port": int(n.port), "uid": getattr(n.uid, "value", None)}
        except Exception:
            return None

    def _get_successor_candidates_local(self):
        """
        ローカルに保持している候補（successor_list + finger）を集める。
        successor が死んでいてもここから昇格できるようにする。
        """
        cands = []
        # successor_list
        try:
            with self._succ_list_lock:
                sl = list(getattr(self, "successor_list", []) or [])
        except Exception:
            sl = list(getattr(self, "successor_list", []) or [])
        for n in sl:
            if n and n != self:
                cands.append(n)
        # fingers（軽く混ぜる）
        try:
            for f in getattr(self, "fingers", [])[:]:
                rn = getattr(f, "respNode", None)
                if rn and rn != self:
                    cands.append(rn)
        except Exception:
            pass
        return cands
    
    
    def return_FT(self):
        '''
        現在のFingerTableをリスト＋辞書形式で返す。
        [{'key':key, 'respNode':respNode.asdict()},...]
        '''
        count_message('return_FT')
        return [{'key':f.key, 'respNode':f.respNode.asdict()} for f in self.fingers]

    
    def return_cash(self):
        '''
        searchedの内容を返す．RPCだとリストに変換される
        '''
        count_message('return_cash')
        return self.searched
    
    def return_range_of_key(self):
        '''RPCを想定。経路表からこのサーバが担当する現在のkeyの範囲を返す.返り値はどちらもstr型.
        '''
        count_message('return_range_of_key')
        return self.predecessor.uid+1, self.uid.value
    
    def return_all_contents(self):
        '''
        現在のcontentsの内容を辞書+リスト形式で返す。
        {key(str):[コンテンツのdict,...],...}
        '''
        count_message('return_all_contents')
        return self.contents_as_RPC(self.contents) 
    
    def delete_all_contents(self):
        '''
        現在のサーバのコンテンツの内容, クエリキャッシュを全て削除する
        '''
        count_message('delete_all_contents')
        self.contents={}
        self.searched=set()
        log.info("%s - deleted all contents & cashes on this server" %(self.uid))
    
    def delete_all_cashes(self):
        '''
        現在のサーバのクエリキャッシュを全て削除する
        '''
        count_message('delete_all_cashes')
        self.searched=set()
        log.info("%s - deleted all cashes on this server" %(self.uid))
    
    def delete_all_cashes_servers(self):
        '''
        sucを辿ってすべてのサーバから現在のクエリキャッシュを削除する
        '''
        visited=set()
        cNode,hop =self.get_NodeInterface(self.asdict())
        while cNode.uid.value not in visited:
            visited.add(cNode.uid.value)
            cNode.methodProxy.delete_all_cashes()
            cNode,hop=self.get_NodeInterface(cNode.methodProxy.get_successor())
    
    def delete_all_contents_servers(self):
        '''
        sucを辿ってすべてのサーバからコンテンツ,クエリキャッシュを消す
        '''
        visited=set()
        cNode,hop =self.get_NodeInterface(self.asdict())
        while cNode.uid.value not in visited:
            visited.add(cNode.uid.value)
            cNode.methodProxy.delete_all_contents()
            cNode,hop=self.get_NodeInterface(cNode.methodProxy.get_successor())

    
    def contents_as_RPC(self, contents:dict):
        '''
        指定されたコンテンツ一覧(辞書型)の中身を変換。
        contentsはdict型にContents型のリストが入ってるのでRPCで送れるようdict型に直す
        元：{key(str):[data(Contents),data,...]}
        後：{key(str):[{'data':data, 'uid':uid(str)},...]}
        '''
        rdict={}
        for key,l in contents.items():
            rdict[key]=[c.asdict() for c in l]
        return rdict

    """
    def reset_message_bytes(self):
        
        メッセージバイト数をリセットするRPC対応メソッド
        
        for key in clientxmlrpc.message_bytes_sent:
            clientxmlrpc.message_bytes_sent[key] = 0
            clientxmlrpc.message_bytes_received[key] = 0
        return True
    
    def get_message_bytes(self):
        
        メッセージバイト数を取得するRPC対応メソッド
        
        return {
            'sent': clientxmlrpc.message_bytes_sent.copy()
            'received': clientxmlrpc.message_bytes_received.copy()
        }
    """

    def reset_message_counts(self):
        """このノードのカウントをリセット（RPCで呼べる）"""
        # serverxmlrpc の reset を呼ぶ
        from serverxmlrpc import reset_message_counts_global
        return reset_message_counts_global()

    def get_message_counts(self):
        """このノードのカウントを返す（RPCで呼べる）"""
        from serverxmlrpc import get_message_counts_snapshot
        return get_message_counts_snapshot()

    def get_total_message_count(self):
        """このノードの合計カウントを返す（RPCで呼べる）"""
        from serverxmlrpc import get_total_message_count_snapshot
        return get_total_message_count_snapshot()

    def get_message_counts_all(self):
        """
        リング全体のメッセージカウントを集計して返す（RPCで呼べる）。
        successor を辿って全ノードを訪問します（到達不能ノードがあれば途中で終了）。
        """
        from collections import defaultdict
        total = defaultdict(int)
        visited = set()

        # start from this node's NodeInterface
        cNode, _ = self.get_NodeInterface(self.asdict())
        while cNode.uid.value not in visited:
            visited.add(cNode.uid.value)
            try:
                counts = cNode.methodProxy.get_message_counts()
            except Exception as e:
                # あるノードへのRPC失敗時は警告を出して巡回を止める／続ける選択はここで
                log.warning(f"{self.uid} - failed to get_message_counts from {cNode.uid}: {e}")
                break
            for k, v in counts.items():
                total[k] += v
            try:
                succ_dict = cNode.methodProxy.get_successor()
            except Exception as e:
                log.warning(f"{self.uid} - failed to get_successor from {cNode.uid}: {e}")
                break
            cNode, _ = self.get_NodeInterface(succ_dict)
        return dict(total)

    def get_exploration_hops(self):
        """全体の探索ホップ総数を取得（RPCで呼べる）"""
        return get_exploration_hops_snapshot()

    def reset_exploration_hops(self):
        """全体の探索ホップ総数をリセット（RPCで呼べる）"""
        return reset_exploration_hops_global()

    def put_content(self, content: dict):
        count_message('put_content')

        rslt = self.find_successor(content['uid'])
        succNode_if, hop = self.get_NodeInterface(rslt)

        log.debug("%s - try store content %s on node %s",
                self.uid, content['uid'][:10], rslt.get('uid','?')[:9])

        # neighbors を使わない通常PUTは primary 経由に寄せる
        if (not content.get('neighbors')) or isinstance(content['neighbors'][0], dict):
            method_name = 'store_content_primary'
            args = (content,)
        else:
            method_name = 'store_contentP'
            args = (content,)

        try:
            # PUTは「速いACK」が最優先：fallback で successor を辿らない
            method = getattr(succNode_if.methodProxy, method_name)
            ret = self._safe_rpc_call(method, *args, timeout=RPC_PER_CALL_TIMEOUT)

            if isinstance(ret, dict) and ret.get("ok") is True and ret.get("uid") == content.get("uid"):
                return ret

            log.warning("%s - store returned non-ACK: uid=%s ret=%r",
                        self.uid, content['uid'][:10], ret)
            return {"ok": False, "uid": content.get("uid"), "err": f"bad_ack:{repr(ret)[:200]}"}

        except Exception as e:
            log.warning("%s - failed to store content %s: %s",
                        self.uid, content['uid'][:10], e)
            return {"ok": False, "uid": content.get("uid"), "err": f"{type(e).__name__}: {e}"}

    
    def store_content(self, content: dict):
        """
        主にRPC経由でデータを自サーバに保存する（replica / handoff / repair / join_pull もここに流す）
        返り値:
        ok=True の場合も、applied/dedup/updated を明示して返す（統計のため）
        """
        try:
            raw = content  # keep original dict

            # --- extract meta (optional) ---
            meta = {}
            try:
                if isinstance(raw, dict):
                    meta = raw.get("__meta") or {}
            except Exception:
                meta = {}

            reason = None
            src = None
            event_id = None
            try:
                reason = meta.get("reason")
                src = meta.get("src")
                event_id = meta.get("event")
            except Exception:
                reason = None

            # normalize reason
            if reason not in ("handoff", "join_pull", "repair", "put_replica", "put_primary"):
                reason = "unknown"

            # --- recv attempt count (receiver-side ground truth) ---
            try:
                with self._transfer_lock:
                    self.transfer_stats[f"{reason}_recv_attempt"] += 1
            except Exception:
                pass

            # version is optional
            ver = None
            try:
                if isinstance(raw, dict):
                    ver = raw.get("ver")
            except Exception:
                ver = None
            try:
                ver_i = int(ver) if ver is not None else 0
            except Exception:
                ver_i = 0

            # build Contents
            cobj = Contents(raw['gid'], raw['id'], raw['data'], raw['uid'], False, raw.get('neighbors'))
            try:
                setattr(cobj, "ver", ver_i)
            except Exception:
                pass

            key = cobj.uid.value

            # --- apply logic: (uid,gid,id) existing? compare ver ---
            applied = False
            updated = False
            dedup = False

            if key in self.contents:
                existing_list = self.contents[key]

                # find same (gid,id)
                target = None
                for ex in existing_list:
                    if getattr(ex, "gid", None) == cobj.gid and getattr(ex, "id", None) == cobj.id:
                        target = ex
                        break

                if target is not None:
                    old_ver = 0
                    try:
                        old_ver = int(getattr(target, "ver", 0) or 0)
                    except Exception:
                        old_ver = 0

                    # ver が同じ/古い -> 適用しない（dedup扱い）
                    if ver_i <= old_ver:
                        dedup = True
                        applied = False
                    else:
                        # 新しいver -> update適用
                        try:
                            target.data = cobj.data
                            target.neighbors = getattr(cobj, "neighbors", None)
                        except Exception:
                            pass
                        try:
                            setattr(target, "ver", ver_i)
                        except Exception:
                            pass
                        applied = True
                        updated = True
                else:
                    # same uid but different entry -> collision like; append
                    existing_list.append(cobj)
                    applied = True
            else:
                self.contents[key] = [cobj]
                applied = True

            # --- recv applied count (only when actually stored/updated) ---
            if applied:
                try:
                    with self._transfer_lock:
                        self.transfer_stats[f"{reason}_recv_applied"] += 1
                        if updated:
                            self.transfer_stats[f"{reason}_recv_updated"] += 1
                        else:
                            self.transfer_stats[f"{reason}_recv_new"] += 1
                except Exception:
                    pass

            log.debug("%s - stored content %s on this node (applied=%s, ver=%s, reason=%s)",
                    self.uid, key[:20], applied, ver_i, reason)

            return {
                "ok": True,
                "uid": key,
                "ver": ver_i,
                "applied": applied,
                "updated": updated,
                "dedup": dedup,
                "reason": reason,
                "src": src,
                "event": event_id,
                "stored_on": getattr(self, "ip", None) or getattr(self, "addr", None) or "unknown",
                "stored_len": len(self.contents.get(key, [])),
            }

        except Exception as e:
            uid = None
            try:
                if isinstance(content, dict):
                    uid = content.get("uid")
                else:
                    uid = getattr(getattr(content, "uid", None), "value", None)
            except Exception:
                uid = None
            return {"ok": False, "uid": uid, "err": f"{type(e).__name__}: {e}"}


    def store_contentP(self, content: dict):
        """
        主にRPC経由でデータを自サーバに保存する。ContentsP型用
        """
        try:
            content = ContentsP(content['gid'], content['id'], content['data'], content['uid'], False, content['neighbors'])
            if content.uid.value in self.contents:
                self.contents[content.uid.value].append(content)
            else:
                self.contents[content.uid.value] = [content]
            log.debug("%s - stored content %s on this node" % (self.uid, content.uid))

            # ACK
            return {
                "ok": True,
                "uid": content.uid.value,
                "stored_on": getattr(self, "ip", None) or getattr(self, "addr", None) or "unknown",
                "stored_len": len(self.contents.get(content.uid.value, [])),
            }
        except Exception as e:
            return {"ok": False, "uid": content.get("uid"), "err": f"{type(e).__name__}: {e}"}

    
    def get_content_from_here(self, key: str, hop: int = 1, rHop: int = 1, probe: int = 2):
        """
        高速版：このノードを primary とみなして取得を試みる（routingしない）
        - まず自分の contents を見る
        - なければ successor_list 上位(probe-1台)へ return_content_one を投げる
        戻り値:
        - 見つかれば [ {uid,data,ver,hop,rHop} ]
        - 無ければ [hop, rHop]
        """
        count_message("get_content_from_here")

        # 1) local
        try:
            local = self.return_content_one(key, hop, rHop)
            if isinstance(local, list) and local and isinstance(local[0], dict):
                return local
        except Exception:
            pass

        # 2) successor_list 上位へ probe（自分以外で最大 probe-1 台）
        try:
            with self._succ_list_lock:
                slist = list(getattr(self, "successor_list", []) or [])
        except Exception:
            slist = list(getattr(self, "successor_list", []) or [])

        tried = 0
        for n in slist:
            if tried >= max(0, int(probe) - 1):
                break
            try:
                d = self._ni_to_dict(n)
                if not d or not d.get("ip") or not d.get("port"):
                    continue
                n_if, _ = self.get_NodeInterface(d)
                resp = self._rpc_call_with_fallback(
                    n_if,
                    "return_content_one",
                    (key, hop, rHop),
                    per_call_timeout=RPC_PER_CALL_TIMEOUT,
                    overall_timeout=ROUTING_OVERALL_TIMEOUT,
                    retries=1,
                )
                tried += 1
                if isinstance(resp, list) and resp and isinstance(resp[0], dict):
                    return resp
            except Exception:
                tried += 1
                continue

        return [hop, rHop]

    
    def get_content(self, key: str):
        '''
        クライアントがRPCを使って特定のkeyに対応するデータを取得する。ホップ数も要素に含めて返す。
        rHop: 実際のhop数。hop: Chord理論で必要な最低限の通信hopのみ
        返り値は list[dict] または [hop, rHop] (miss/fail) を推奨（XMLRPC互換で安定）
        '''
        count_message('get_content')

        # key が str でないケースも安全にログる
        try:
            key_head = (key[:8] if isinstance(key, str) else str(key)[:8])
        except Exception:
            key_head = "????"

        # log.warning(f"[GET_ENTER] {self.uid} key={key_head}")

        rHop = 1  # this RPC itself counts as 1
        hop = 1

        # XMLRPC では tuple が list 化されたりするので、miss/fail は明示的に list で返すと client 側判定が安定
        def _miss():
            return [hop, rHop]

        if not getattr(self, "predecessor", None) or not getattr(self, "successor", None):
            return _miss()

        # --- S1: find_successor ---
        # log.warning(f"[GET_S1] {self.uid} find_successor start key={key_head}")
        rslt = {
            "ip": self.ip,
            "port": self.port,
            "uid": str(self.uid) if not isinstance(self.uid, str) else self.uid,
            "hop": 1,
            "rHop": 1,
        }
        try:
            rslt = self.find_successor(key)
        except Exception as e:
            # log.warning(f"[GET_S1_FAIL] {self.uid} find_successor exc={type(e).__name__}: {e}")
            return _miss()

        if not isinstance(rslt, dict):
            # log.warning(f"[GET_S1_BAD] {self.uid} find_successor returned {type(rslt).__name__}")
            return _miss()

        # rslt が壊れている/キーが無いケースでも落ちないように
        try:
            rHop += int(rslt.get('rHop', 0) or 0)
            hop += int(rslt.get('hop', 0) or 0)
        except Exception:
            pass

        # try:
            # log.warning(f"[GET_S1_DONE] {self.uid} succ={str(rslt.get('ip'))}:{str(rslt.get('port'))}")
        # except Exception:
            # log.warning(f"[GET_S1_DONE] {self.uid} succ=<unknown>")

        # --- S2: get_NodeInterface (一度だけ) ---
        # log.warning(f"[GET_S2] {self.uid} get_NodeInterface")
        succNode_if = None
        hopDiff = 0
        try:
            succNode_if, hopDiff = self.get_NodeInterface(rslt)
            rHop += int(hopDiff or 0)
        except Exception as e:
            # log.warning(f"[GET_S2_FAIL] {self.uid} get_NodeInterface exc={type(e).__name__}: {e}")
            return _miss()

        # try:
            # log.warning(f"[GET_S2_DONE] {self.uid} succNI={succNode_if.ip}:{succNode_if.port}")
        # except Exception:
            # log.warning(f"[GET_S2_DONE] {self.uid} succNI=<unknown>")

        # successor-list based fallback (best-effort)
        candidates = []
        try:
            candidates.append(rslt)
        except Exception:
            pass

        # 2) ローカル successor_list を必ず候補に入れる
        try:
            with self._succ_list_lock:
                local_slist = list(getattr(self, "successor_list", []) or [])
        except Exception:
            local_slist = list(getattr(self, "successor_list", []) or [])

        for n in local_slist:
            try:
                d = self._ni_to_dict(n)
                if d and d.get("ip") and d.get("port"):
                    candidates.append(d)
            except Exception:
                continue

        # 3) primary が alive のときだけ primary から successor_list を pull if succNode_if and self._is_node_alive(succNode_if):
        ENABLE_GET_PULL_SLIST = False
        try:
            if ENABLE_GET_PULL_SLIST:
                succ_list = self._rpc_call_with_fallback(
                    succNode_if,
                    'get_successor_list',
                    tuple(),
                    per_call_timeout=RPC_PER_CALL_TIMEOUT,
                    overall_timeout=ROUTING_OVERALL_TIMEOUT,
                    retries=1,
                )
                if isinstance(succ_list, list):
                    for d in succ_list:
                        if isinstance(d, dict) and d.get('ip') and d.get('port'):
                            candidates.append(d)
        except Exception:
            pass

        # de-dup candidates
        uniq = []
        seen = set()
        for d in candidates:
            try:
                k = d.get('uid') or f"{d.get('ip')}:{d.get('port')}"
            except Exception:
                continue
            if k in seen:
                continue
            seen.add(k)
            uniq.append(d)
            if len(uniq) >= 2:
                break

        # try each candidate as a replica source
        last_err = None
        responses = []   # list of list[dict]

        for d in uniq:
            try:
                n_if, _ = self.get_NodeInterface(d)
                resp = self._rpc_call_with_fallback(
                    n_if,
                    'return_content_one',
                    (key, hop, rHop + int(hopDiff or 0)),
                    per_call_timeout=RPC_PER_CALL_TIMEOUT,
                    overall_timeout=ROUTING_OVERALL_TIMEOUT,
                    retries=1,
                )

                # return_contents() 成功: list[dict], 失敗: [hop,rHop] や tuple/empty
                if isinstance(resp, list) and resp and isinstance(resp[0], dict):
                    return resp
            except Exception as e:
                last_err = e
                continue

        if not responses:
            log.warning(f"{self.uid} - get_content failed for key {key_head}: {last_err}")
            return _miss()

        # choose dict with maximum 'ver'
        best = None
        best_ver = -1
        for lst in responses:
            for c in lst:
                if not isinstance(c, dict):
                    continue
                try:
                    v = int(c.get("ver", 0) or 0)
                except Exception:
                    v = 0
                if v > best_ver:
                    best_ver = v
                    best = c

        if best is None:
            log.warning(f"{self.uid} - get_content no valid replica for key {key_head}")
            log.warning(f"[GET_EXIT] {self.uid} key={key_head}")
            return _miss()

        # hop/rHop を補う
        try:
            if "hop" not in best:
                best["hop"] = hop
            if "rHop" not in best:
                best["rHop"] = rHop
        except Exception:
            pass

        log.warning(f"[GET_EXIT] {self.uid} key={key_head}")
        return [best]

    

    def return_multiple_contents(self, keyList:list,hop:int,rHop:int):
        """主にRPC経由で指定されたキー群を持つデータのリストを返す
        [dict,dict,dict,...] Contents型をRPCで渡せる形(dict)に変換した上で渡す
        該当キーがなければhop数のみを返す(多分tuple扱い)
        一つ目の要素に指定されたhop数を登録する。
        聞かれたノードが1個もなければhop数のみが返る

        Args:
            keyList (list): 対象のkey群(str)のリスト。聞かれたノードがなければ返さない
            hop (int): 論理的に最小のhop数
            rHop (int): 実際のRPC通信の数
        """
        ctsList=[]
        for i,key in enumerate(keyList):
            if i==0:
                rcnts=self.return_contents(key,hop,rHop)
            else:
                rcnts=self.return_contents(key,0,0)
            if isinstance(rcnts[0],dict): #1つ以上コンテンツが見つかった
                ctsList.extend(rcnts)
        if len(ctsList)==0: #1個もコンテンツが見つからなかった
            return hop,rHop
        else:
            return ctsList
    
    def return_contents(self, key:str,hop:int,rHop:int):
        '''
        主にRPC経由で指定されたキーを持つデータのリストを返す
        [dict,dict,dict,...] Contents型をRPCで渡せる形(dict)に変換した上で渡す
        該当キーがなければhop数のみを返す(多分tuple扱い)
        '''
        count_message('return_contents')
        if key in self.contents:
            ctsList=[]
            for i,content in enumerate(self.contents[key]):
                cDict=content.asdict(withNeighbors=False, adj=0)
                # include version in GET response (added for successor-list replication)
                try:
                    if 'ver' not in cDict:
                        cDict['ver'] = int(getattr(content, 'ver', 0) or 0)
                except Exception:
                    cDict['ver'] = 0
                if i==0: #初回のみ実値
                    cDict['hop']=hop
                    cDict['rHop']=rHop
                else: #2個目以降の要素はホップ数かかってないので入れない
                    cDict['hop']=0
                    cDict['rHop']=0
                ctsList.append(cDict)
            return ctsList
        else:
            return hop,rHop
    
    def return_content_one(self, key: str, hop: int, rHop: int):
        """
        return_contents の軽量版: 1件だけ返す（ver最大）
        戻り値は XMLRPC 安定のため list で返す:
        - 見つかれば [ {uid,data,ver,hop,rHop} ]
        - 無ければ [hop, rHop]
        """
        count_message('return_content_one')

        try:
            lst = self.contents.get(key)
            if not lst:
                return [hop, rHop]

            best = None
            best_ver = -1
            for c in lst:
                try:
                    v = int(getattr(c, 'ver', 0) or 0)
                except Exception:
                    v = 0
                if v > best_ver:
                    best_ver = v
                    best = c

            if best is None:
                return [hop, rHop]

            return [{
                "uid": best.uid.value,
                "data": best.data,
                "ver": best_ver,
                "hop": hop,
                "rHop": rHop,
            }]
        except Exception:
            return [hop, rHop]

    

    def move_contents(self):
        '''predの更新時(大体は新サーバ参加時)に新predのID<key<=self.idに一致しないkeyのcontentsは自サーバから新predへ移る
        #TODO ほんとはサーバ離脱時の処理も必要だが後で実装
        '''
        if len(self.contents)!=0: #dataが空じゃないなら
            toSend={} #{key:[Contents,...]}
            #管理範囲外のデータは新predに渡す用のdictにいれる
            for key, value in self.contents.items():
                if not Key(key).is_between_r_inclu(self.predecessor.uid.value, self.uid.value):
                    toSend[key]=value
            for cs in toSend.values():
                for c in cs:
                    self.put_content(c.asdict())

    def export_contents_in_range(self, start_uid_hex: str, end_uid_hex: str, delete: bool = False):
        """指定レンジ(start_uid_hex, end_uid_hex] に属するcontentsを export する。
        - start_uid_hex/end_uid_hex は **UIDのhex文字列**（hash済み）を想定（ここで再hashしない）
        - 返り値: list[dict] (content.asdict())
        - delete=True のときはローカルからも削除（= handoff 用）
        """
        count_message('export_contents_in_range')
        try:
            start_uid_hex = str(start_uid_hex)
            end_uid_hex = str(end_uid_hex)
        except Exception:
            pass

        exported = []
        # contents: {key_hex: [Content, ...]} を想定
        for key_hex, clist in list(getattr(self, "contents", {}).items()):
            try:
                if not Key(key_hex).is_between_r_inclu(start_uid_hex, end_uid_hex):
                    continue
            except Exception:
                continue

            for c in list(clist):
                try:
                    exported.append(c.asdict() if hasattr(c, "asdict") else c)
                except Exception:
                    # 最低限 key だけでも返す
                    try:
                        exported.append({"key": key_hex, "value": c})
                    except Exception:
                        pass

            if delete:
                try:
                    del self.contents[key_hex]
                except Exception:
                    pass

        return exported


        out = []
        # self.contents: {key(str hex): [Contents/ContentsP,...]}
        for key_hex, lst in list(self.contents.items()):
            try:
                # (start, end] 判定（右含む）
                if Key(key_hex).is_between_r_inclu(start_uid.value, end_uid.value):
                    for c in lst:
                        out.append(c.asdict())
            except Exception:
                pass
        return out

    def pull_from_successor_after_join(self, delete_from_succ: bool = False, retries: int = 5, delay: float = 0.2):
        """再参加/参加直後のフォールバックとして、successor から自分の担当範囲 (pred, self] のキーを pull する。
        本来は successor 側が notify_new_predecessor で push するのがChordらしいが、
        ring がまだ収束していない局面の保険として残す。
        """
        count_message('pull_from_successor_after_join')
        if getattr(self, "successor", None) is None or self.successor == self:
            log.info(f"{self.uid} - pull skipped: no successor")
            return 0

        start_uid_hex = None
        try:
            if getattr(self, "predecessor", None):
                start_uid_hex = self.predecessor.uid.value
        except Exception:
            start_uid_hex = None
        if start_uid_hex is None:
            # predecessor が未確定のときは pull しても誤るのでスキップ
            log.info(f"{self.uid} - pull skipped: predecessor is None")
            return 0

        end_uid_hex = self.uid.value
        pulled = 0

        for attempt in range(max(1, int(retries))):
            try:
                items = self.successor.methodProxy.export_contents_in_range(start_uid_hex, end_uid_hex, delete_from_succ)
                if not items:
                    log.info(f"{self.uid} - pull: no items from successor for range ({start_uid_hex[:8]}, {end_uid_hex[:8]}] (attempt {attempt+1}/{retries})")
                else:
                    for cdict in items:
                        try:
                            if isinstance(cdict, dict):
                                m = dict(cdict.get("__meta") or {})
                                m["reason"] = "join_pull"
                                cdict["__meta"] = m
                            self.store_content(cdict)  # ローカルに格納（ルーティングしない）
                            pulled += 1
                        except Exception as e:
                            log.warning(f"{self.uid} - pull store_content failed: {e}")
                    log.info(f"{self.uid} - pull: got {len(items)} items from successor (attempt {attempt+1}/{retries})")
                    break
            except Exception as e:
                log.warning(f"{self.uid} - pull RPC failed: {e} (attempt {attempt+1}/{retries})")

            try:
                time.sleep(float(delay))
            except Exception:
                pass

        return pulled
    
    def asdict(self):
        """主にRPC用にノードの情報を返す。
        RPCの返り値はプリミティブ型のみなのでdictが返り値
        BasicNode型からsuccが加わっている点が違い

        Returns:
            dict: ip,port,uid,succ(sucのdict)
        """
        return {"ip": self.ip,
                "port": self.port,
                "uid": self.uid.value,
                "succ": self.fingers[0].respNode.asdict()}

    def stop(self):
        # 0) 多重 stop 防止
        if getattr(self, "_stopping", False):
            return True
        self._stopping = True

        if hasattr(self, "_stop_event"):
            self._stop_event.set()

        log.info(f"{self.uid} - Node is stopping. Initiating data transfer...")

        # 1) stabilizer を止める（RPCを発生させるので最優先で止める）
        try:
            # どっちの名前が正か環境差があるので両対応
            stab = getattr(self, "stabilizer", None) or getattr(self, "_stabilizer", None)
            if stab:
                if hasattr(stab, "stop"):
                    stab.stop()
                if hasattr(stab, "join"):
                    stab.join(timeout=2.0)
        except Exception as e:
            log.warning(f"{self.uid} - Stabilizer stop failed: {e}")

        # 2) RPCサーバを「closing」にして新規受付を止める（実装があるなら）
        # shutdown_graceful() まではやらない。handoff中は outbound RPC をしたい。
        try:
            if getattr(self, "_rpc_server", None) is not None and hasattr(self._rpc_server, "mark_closing"):
                self._rpc_server.mark_closing()
        except Exception:
            pass

        # 3) successor へ contents を同期RPCで handoff（Executor は使わない）
        succ = None
        try:
            succ = getattr(self, "successor", None)
        except Exception:
            succ = None

        if succ and getattr(succ, "uid", None) and succ.uid != self.uid:
            # 送るコンテンツをフラット化
            all_contents = []
            try:
                for lst in self.contents.values():
                    all_contents.extend(lst)
            except Exception:
                all_contents = []

            content_count = len(all_contents)
            log.info(f"{self.uid} - Transferring {content_count} items to successor {succ.uid}")

            ok = 0
            ng = 0

            import socket
            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(RPC_PER_CALL_TIMEOUT)

            try:
                for content in all_contents:
                    d = content.asdict()
                    try:
                        m = dict(d.get("__meta") or {})
                        m["reason"] = "handoff"
                        m["src"] = f"{self.ip}:{int(self.port)}"
                        d["__meta"] = m
                    except Exception:
                        pass
                    try:
                        is_p = (content.__class__.__name__ == "ContentsP")
                        method = succ.methodProxy.store_contentP if is_p else succ.methodProxy.store_content
                        # store_content の引数は (content_dict,) 1つ
                        method(d)
                        ok += 1
                    except Exception as e:
                        ng += 1
                        log.warning(f"{self.uid} - Failed to transfer content {getattr(content,'uid',None)}: {e}")
            finally:
                socket.setdefaulttimeout(old_timeout)

            log.info(f"{self.uid} - Handoff finished: ok={ok}, ng={ng}")
        else:
            log.info(f"{self.uid} - No valid successor for handoff (succ is None or self).")

        # 4) best-effort notify（必要最小限）
        notify_targets = set()
        try:
            if getattr(self, "successor", None) and self.successor != self:
                notify_targets.add((self.successor.ip, int(self.successor.port)))
            if getattr(self, "predecessor", None) and self.predecessor != self:
                notify_targets.add((self.predecessor.ip, int(self.predecessor.port)))

            for f in getattr(self, "fingers", [])[:]:
                rn = getattr(f, "respNode", None)
                if rn and rn.ip != self.ip:
                    notify_targets.add((rn.ip, int(rn.port)))
        except Exception:
            pass

        for nip, nport in notify_targets:
            try:
                proxy = clientxmlrpc.create_proxy(nip, nport, timeout=3)
                proxy.notify_node_depart(self.asdict())
            except Exception:
                pass

        # === transfer_stats archive push (DEPART backup) ===
        try:
            snap = self.get_transfer_stats()
            ts = time.time()

            archive_targets = set()

            # successor / predecessor
            if getattr(self, "successor", None) and self.successor.uid != self.uid:
                archive_targets.add((self.successor.ip, int(self.successor.port)))
            if getattr(self, "predecessor", None) and self.predecessor.uid != self.uid:
                archive_targets.add((self.predecessor.ip, int(self.predecessor.port)))

            # fingers（冗長化）
            for f in getattr(self, "fingers", []):
                rn = getattr(f, "respNode", None)
                if rn and rn.uid != self.uid:
                    archive_targets.add((rn.ip, int(rn.port)))

            for ip, port in archive_targets:
                try:
                    proxy = clientxmlrpc.create_proxy(ip, port, timeout=3)
                    proxy.store_departed_transfer_stats(self.uid, snap, ts)
                except Exception:
                    pass
        except Exception:
            pass

        
        # 5) ここでRPCサーバを完全停止（active request が落ちるまで少し待つ）
        try:
            if getattr(self, "_rpc_server", None) is not None and hasattr(self._rpc_server, "shutdown_graceful"):
                self._rpc_server.shutdown_graceful(timeout=2.0)
        except Exception as e:
            log.warning("rpc_server.shutdown_graceful failed: %s", e)

        # 6) 最後に Executor shutdown（絶対最後）
        try:
            shutdown_rpc_executor("LocalNode.stop()")
        except Exception:
            pass

        return True



    def create_finger_table(self):
        """
        初期化時のみ呼ばれる。FTのエントリ数はキーのbit数と同じ
        初期状態は自分しかいないのでどのkeyに対応するrespNodeにも自身が入る=sucもself
        ft[0]=self.uid+2^0,self,selfのNodeInterface
        ft[1]=self.uid+2^1,self,selfのNodeInterface
        ...
        """
        selfinterface = NodeInterface(self)
        for i in range(0, self.uid.idlength): #sha256なら256
            self.fingers.append(Finger(self.calcfinger(i), self, selfinterface))

    def set_successor(self, successor:dict):
        """与えられたsuc情報を元に、自身のsucのNodeInterfaceをftにセットする。

        Args:
            successor (dict): {'ip':str, 'port':str}
        """
        self.fingers[0].set_RespNode(self,successor)
    
    def set_predecessor(self, predecessor:dict):
        """与えられたpred情報を元に、自身のpredのNodeInterfaceをセットする。
        ついでに新predへのデータの移譲も行う
        Create a NodeInterface object and set to self.predecessor

        ・ predecessor: dict with ip and port as key
        """
        if not predecessor or not isinstance(predecessor, dict) or not predecessor.get('ip'):
            log.warning("%s - set_predecessor(None) CALLED", self.uid)
            log.warning("%s - set_predecessor(None) stack:\n%s",
                    self.uid, "".join(traceback.format_stack(limit=40)))
            self.predecessor = None
            return
        log.info("%s - set_predecessor(%s:%s uid=%s)",
             self.uid, predecessor.get('ip'), predecessor.get('port'), predecessor.get('uid','?')[:9])
        self.predecessor,hop = self.get_NodeInterface(predecessor)
        self.move_contents()

    def get_successor(self):
        """自身のsucの情報をdictで取得

        Returns:
            dict: ip,port,uid,succ(←sucのsuc)
        """
        return self.fingers[0].respNode.asdict()

    def get_predecessor(self):
        """自身のpred(NodeIF)の情報をdictで。RPC用

        Returns:
            dict: ip,port,uid,succ(←sucのsuc)
        """
        if self.predecessor:
            return self.predecessor.asdict()
        else:
            return None

    def get_NodeInterface(self, nodedict:dict):
        """nodedictのipに基づいてノードのインターフェース型&ホップ数を返す。ip,portが必要
        引数：{'ip':str,'port':int}
        ノードがRPCか否かをユーザが意識しなくていいようにこのメソッドを介す
        """
        #どっちの型を渡すかでNodeInterface型の挙動が変わるため
        if nodedict["ip"] == self.ip and nodedict["port"] == self.port:
            return NodeInterface(self),0 #自身を返してるのでホップ数は0
        else:
            return NodeInterface(nodedict),1 #RPCなのでhop数は1

    def _in_between(self, x, a, b, inclusive_right=False):
        """
        判定: ID x がaとbの間にあるか
        inclusive_right=Trueの場合はbを含む
        a,b,xは整数(UID)を想定
        """
        try:
            # Chordのリング空間はmodulo 2^m
            m = 256
            mod = 2 ** m

            if a < b:
                return (a < x < b) or (inclusive_right and x == b)
            else:
                # wrap-around case
                return (x > a) or (x < b) or (inclusive_right and x == b)
        except Exception as e:
            import logging
            logging.getLogger('chordLog').warning(f"_in_between failed: {e}")
            return False
    
    def join(self, node:NodeInterface):
        """FTの初期化や他ノードへの通知も行う完全版
        RPC不可
        Join method as described in the 4th paragraph(元論文の)
        """
        self.init_fingers(node)
        self.update_others()

        # after init_fingers() and initial update_others()
        # wait for stabilization: call stabilize and fix_fingers periodically, up to timeout
        stabilize_start = time.time()
        STABILIZE_TIMEOUT = 5.0
        while time.time() - stabilize_start < STABILIZE_TIMEOUT:
            try:
                self.stabilize()
                self.fix_fingers()
                time.sleep(0.5)
                # quick check: if successor and predecessor both present and resonable, break
                if getattr(self, 'successor', None) and getattr(self, 'predecessor', None):
                    break
            except Exception:
                time.sleep(0.5)
        
        # after reasonably stabilized, notify known nodes so they include this node in their tables
        # collect a set of candidate nodes to notify: successor, predecessor, and non-None finger entries
        notify_targets = set() 
        try:
            if hasattr(self, 'successor') and self.successor and self.successor != self:
                notify_targets.add((self.successor.ip, int(self.successor.port)))
            if hasattr(self, 'predecessor') and self.predecessor and self.predecessor != self:
                notify_targets.add((self.successor.ip, int(self.predecessor.port)))
            for f in getattr(self, 'ft', [])[:]:
                if f and f != self:
                    try:
                        notify_targets.add((f.ip, int(f.port)))
                    except Exception:
                        pass
        except Exception:
            pass
        
        # notify each target (best-effort, with retries)
        for nip, nport in notify_targets:
            try:
                # create proxy using clientxmlrpc factory if available
                try:
                    import clientxmlrpc
                    proxy = clientxmlrpc.create_proxy(nip, int(nport), timeout=3)
                    proxy.notify_node_join(self.asdict())
                except Exception:
                    # fallback: try direct NodeInterface if accessible on this process
                    try:
                        # If NodeInterface exists in process space:
                        nodeIF = self.get_NodeInterface({'ip': nip, 'port': nport})
                        nodeIF.methodProxy.notify_node_join(self.asdict())
                    except Exception:
                        pass
            except Exception:
                pass

        # join() の最後あたり
        try:
            self.pull_from_successor_after_join()
        except Exception as e:
            log.warning(f"{self.uid} - pull after join failed: {e}")


    def join_5(self, nodeToJoin:NodeInterface):
        """sucの設定のみする簡易版。
        Join method as described in 5th paragraph(元論文の)
        """
        self.set_successor(nodeToJoin.methodProxy.find_successor(self.uid))
    
    def notify_node_join(self, node_dict):
        """
        RPCで呼ばれる: 他ノードが参加したことを通知する
        node_dict = {'ip':..., 'port':..., 'uid':...}
        """
        count_message('notify_node_join')
        try:
            newnode = BasicNode(node_dict)  # ここはOK（dict→BasicNode）

            # update_finger_table は dict を期待するので、必ず dict を渡す
            try:
                self.update_finger_table(newnode.asdict(), 0)
            except Exception as e:
                log.warning("notify_node_join: update_finger_table failed: %s", e)

            # successor更新は property 代入ではなく set_successor を使う
            try:
                cur_succ = self.get_successor()  # dict
                # successor が未設定/壊れている場合に備えた best-effort
                if (not cur_succ) or self._in_between(newnode.uid, self.uid, Key(cur_succ['uid']), inclusive_right=True):
                    self.set_successor(newnode.asdict())   # ← finger[0] を更新
            except Exception as e:
                log.warning("notify_node_join: set_successor failed: %s", e)

            log.info("notify_node_join: integrated new node %s:%s", newnode.ip, newnode.port)
        except Exception as e:
            log.warning("notify_node_join failed: %s", e)

        return True

    
    def notify_node_depart(self, node_dict):
        """
        RPCで呼ばれる: 他ノードが離脱したことを通知する
        finger table, successor, predecessorから該当エントリを削除/更新する（best-effort）
        """
        count_message('notify_node_depart')
        try:
            target_ip = node_dict.get('ip') if isinstance(node_dict, dict) else None
            target_port = node_dict.get('port') if isinstance(node_dict, dict) else None
            # clear matches in finger table
            for i in range(len(self.fingers)):
                try:
                    if self.fingers[i] and self.fingers[i].ip == target_ip and int(self.fingers[i].port) == int(target_port):
                        self.fingers[i] = None
                except Exception:
                    pass
            
            # successorの修正
            if self.successor and self.successor.ip == target_ip and int(self.successor.port) == int(target_port):
                log.info(f"{self.uid} - successor {target_ip} departed, trying to find replacement.")
                try:
                    # 離脱ノードのsuccessorを取得してsuccessorを更新
                    departed_node_proxy, _ = self.get_NodeInterface({'ip': target_ip, 'port': target_port})
                    departed_succ = self._safe_rpc_call(departed_node_proxy.methodProxy.get_successor)
                    if departed_succ and departed_succ.get("ip") != self.ip:
                        self.set_successor(departed_succ)
                        # successorに「自分がpredecessor」と通知
                        try:
                            self.successor.methodProxy.notify_new_predecessor(self.asdict())
                        except Exception:
                            pass
                        log.info(f"{self.uid} - successor updated to {departed_succ['ip']} after {target_ip} left.")
                    else:
                        # fallback: 自分でsuccessorを探す
                        new_suc = self.find_successor(self.uid)
                        if new_suc:
                            self.set_successor(new_suc)
                            self.succeessor.methodProxy.notify_new_predecessor(self.asdict())
                except Exception as e:
                    log.warning(f"{self.uid} - failed to find replacement successor for departed {target_ip}: {e}")
            
            # predecessorの修正
            if self.predecessor and self.predecessor.ip == target_ip and int(self.predecessor.port) == int(target_port):
                log.info(f"{self.uid} - predecessor {target_ip} departed, trying to find replacement.")
                try:
                    departed_node_proxy, _ = self.get_NodeInterface({'ip': target_ip, 'port': target_port})
                    departed_pred = self._safe_rpc_call(departed_node_proxy.methodProxy.get_predecessor)

                    if departed_pred and departed_pred.get("ip") != self.ip:
                        self.set_predecessor(departed_pred)
                        # predecessorに「自分がsuccessor」と通知
                        try:
                            proxy_pred, _ = self.get_NodeInterface(departed_pred)
                            proxy_pred.notify_new_successor(self.asdict())
                        except Exception:
                            pass
                        log.info(f"{self.uid} - predecessor updated to {departed_pred['ip']} after {target_ip} left.")
                    else:
                        log.warning(f"{self.uid} - could not retrieve valid predecessor from departed {target_ip}")
                except Exception as e:
                    log.warning(f"{self.uid} - failed to find replacement predecessor for departed {target_ip}: {e}")

            
            log.info(f"notify_node_depart: removed node {target_ip}:{target_port}")
        except Exception as e:
            log.warning(f"notify_node_depart failed: {e}")
        return True

    def stabilize(self):
        '''
        自分の successor が正しいかを検証・更新する。sucの情報が間違ってる場合は修正を促す
        '''
        count_message('stabilize')
        node_inter = None

        # ----------------------------------------
        # 0) successor が死んでいたら、まず successor_list から昇格
        #    （find_successor に依存すると回復不能化しやすい）
        # ----------------------------------------
        try:
            succ = getattr(self, "successor", None)
        except Exception:
            succ = None

        def _promote_from_local_candidates() -> bool:
            for cand in self._get_successor_candidates_local():
                try:
                    if not cand or cand == self:
                        continue
                    # 自分自身を除外
                    if getattr(cand, "uid", None) and cand.uid.value == self.uid.value:
                        continue
                    # alive のみ採用
                    if self._is_node_alive(cand):
                        d = self._ni_to_dict(cand)
                        if d and d.get("ip") and d.get("ip") != self.ip:
                            self.set_successor(d)
                            return True
                except Exception:
                    continue
            return False

        if succ and succ != self:
            try:
                if not self._is_node_alive(succ):
                    log.warning(f"{self.uid} - successor {getattr(succ,'uid',None)} seems dead; promote from successor_list...")
                    if _promote_from_local_candidates():
                        try:
                            self._refresh_successor_list()
                        except Exception:
                            pass
                    else:
                        log.warning(f"{self.uid} - no live candidate in successor_list/fingers (will fallback to find_successor).")
            except Exception:
                pass

        try:
            # successorのチェック
            node_inter = self._safe_rpc_call(self.successor.methodProxy.get_predecessor, timeout=RPC_PER_CALL_TIMEOUT) #自身のsucのpreのdictを取得（＝正しい状態なら自身の情報が入ってるはず）
        except Exception:
            log.warning(f"{self.uid} - successor {self.successor} seems dead. Finding new successor...")
            # successorが応答しない(=離脱)場合の処理
            # 自身のIDのsuccessorを再度検索
            
            # まず local candidates から昇格を再試行
            if not _promote_from_local_candidates():
                # 最後の手段として find_successor
                new_suc = self.find_successor(self.uid)
                if new_suc and new_suc.get("ip") and new_suc["ip"] != self.ip:
                    self.set_successor(new_suc)
                else:
                    log.warning(f"{self.uid} - could not find a valid successor after {self.successor}")
                    return
                # 新しいsuccessorに「自分がpredecessor」であると通知
                try:
                    self.successor.methodProxy.notify_new_predecessor(self.asdict())
                    log.info(f"{self.uid} - updated successor to {new_suc['ip']}, notified it as my new predecessor.")
                except Exception as e:
                    log.warning(f"{self.uid} - failed to notify new successor {new_suc['ip']}: {e}")
                # refresh successor list after successor change
                try:
                    self._refresh_successor_list()
                except Exception:
                    pass

            return

        if node_inter: #node_inter(=sucのpred)がnoneじゃないなら
            if node_inter["uid"] == self.uid: #sucは正しい状態
                pred_uid = self.predecessor.uid if self.predecessor is not None else None
                log.info(f"{self.uid} - my suc:{self.successor.uid}, my pred:{pred_uid}")
                return
            #そうでないと言うことは自身とself.sucの間に別サーバnode_interがある。
            #自身とsucのキーが同じ(初期状態) or node_interのキーがselfとsucの中間にあるならnode_interの方がより正しいsuc。
            # ここに入らない場合、self.sucはあってるがsuc.predがselfより前のキーを指してる
            if self.uid == self.successor.uid\
                    or Key(node_inter["uid"]).is_between_exclu(self.uid, self.successor.uid):
                self.set_successor(node_inter) #自身のsucは修正完了
        #ここに来てるってことはsuc.predにselfが登録されていないので自身を通知．以下のパターンのどれか
        # ①node_interがNone: sucがまだpredを持ってない
        # ②新しいsucを設定: sucはまだselfを知らない
        # ③sucのpredが不正: suc.predがselfより前のキーを指してる

        # predecessorのチェックと更新
        if self.predecessor and self.predecessor.uid != self.uid:
            try:
                self._safe_rpc_call(self.predecessor.methodProxy.get_successor, timeout=RPC_PER_CALL_TIMEOUT)
            except Exception as e:
                # RPC失敗
                log.warning(f"{self.uid} - predecessor {getattr(self.predecessor, 'uid', None)} seems dead; clearing predecessor. err={e}")
                self.predecessor = None

        if self.successor.uid != self.uid:
            try:
                self.successor.methodProxy.notify_new_predecessor(self.asdict())
            except Exception as e:
                log.warning(f"{self.uid} - failed to notify successor {self.successor.uid}: {e}")

        # successor list maintenance (best-effort)
        try:
            self._refresh_successor_list()
        except Exception:
            pass
        
        # replica repair (primary baseline, best-effort)
        try:
            self._repair_replicas_primary()
        except Exception:
            pass

    # ----------------------------
    # repair (primary baseline)
    # ----------------------------
    def _is_primary_for_key(self, key_hex: str) -> bool:
        """Whether this node is the primary responsible for key (pred, self]."""
        try:
            if not getattr(self, "predecessor", None) or getattr(self.predecessor, "uid", None) is None:
                return False
            pred_uid_hex = self.predecessor.uid.value
            return Key(key_hex).is_between_r_inclu(pred_uid_hex, self.uid.value)
        except Exception:
            return False
            
    def _repair_replicas_primary(self):
        """Primary pushes repair (via replica pull) to keep r replicas best-effort."""
        try:
            with self._succ_list_lock:
                replicas = list(self.successor_list)
        except Exception:
            replicas = []
        if not replicas:
            return

        targets = []
        for n in replicas[:REPLICATION_FACTOR]:
            try:
                if n.uid.value == self.uid.value:
                    continue
                targets.append(n)
            except Exception:
                continue
        targets = targets[: max(0, REPLICATION_FACTOR - 1)]
        if not targets:
            return

        try:
            keys = list(getattr(self, "contents", {}).keys())
        except Exception:
            keys = []

        checked = 0
        primary_dict = self._asdict_min()

        # ★ repair event id (stabilize起点)
        try:
            event_id = f"repair@{time.time():.3f}:{self.ip}:{int(self.port)}"
        except Exception:
            event_id = None

        for key_hex in keys:
            if checked >= REPAIR_MAX_KEYS_PER_STABILIZE:
                break
            if not self._is_primary_for_key(key_hex):
                continue

            best_dict, best_ver = self._best_local_content_with_ver(key_hex)
            if best_dict is None or best_ver <= 0:
                continue
            checked += 1

            for rep in targets:
                def _repair_one(rep_if, key_hex=key_hex, expected_ver=best_ver, ev=event_id):
                    try:
                        try:
                            rver = rep_if.methodProxy.replica_get_version(key_hex)
                            rver = int(rver or 0)
                        except Exception:
                            rver = 0
                        if rver >= int(expected_ver):
                            return
                        # ★ event_id を渡す（引数追加）
                        rep_if.methodProxy.repair_pull_from_primary(primary_dict, key_hex, int(expected_ver), ev)
                    except Exception:
                        return

                try:
                    threading.Thread(target=_repair_one, args=(rep,), daemon=True).start()
                except Exception:
                    continue


    def notify_new_predecessor(self, new_predecessor:dict):
        """successor側で predecessor 更新を受け取ったときに呼ばれる。
        Chordの基本挙動に沿って、**predecessor が更新された場合は**
        その新predecessor（=参加してきたノード）に対してキー範囲を handoff する。
        """
        count_message('notify_new_predecessor')

        # new_predecessor が既存の pred と自身の間にいるなら、new_predecessor がより正しい pred
        should_update = (not getattr(self, "predecessor", None)) or Key(new_predecessor["uid"]).is_between_exclu(self.predecessor.uid, self.uid)
        if not should_update:
            return

        # 旧predのUID（handoff範囲の下限）
        try:
            old_pred_uid_hex = self.predecessor.uid.value if getattr(self, "predecessor", None) else self.uid.value
        except Exception:
            old_pred_uid_hex = self.uid.value

        # pred更新
        self.set_predecessor(new_predecessor)

        # ---- Handoff: (old_pred, new_pred] を new_predecessor に渡す ----
        new_pred_uid_hex = str(new_predecessor.get("uid", ""))

        try:
            dest_iface, _ = self.get_NodeInterface(new_predecessor)
        except Exception as e:
            log.warning(f"{self.uid} - cannot build NodeInterface for new predecessor: {e}")
            return

        ok = 0
        ng = 0
        # contents: {key_hex: [Content,...]}
        for key_hex, clist in list(getattr(self, "contents", {}).items()):
            try:
                if not Key(key_hex).is_between_r_inclu(old_pred_uid_hex, new_pred_uid_hex):
                    continue
            except Exception:
                continue

            for c in list(clist):
                try:
                    cdict = c.asdict() if hasattr(c, "asdict") else c
                    dest_iface.methodProxy.store_content(cdict)
                    ok += 1
                except Exception as e:
                    ng += 1
                    log.warning(f"{self.uid} - handoff store_content failed to {new_predecessor.get('ip')}:{new_predecessor.get('port')}: {e}")

            # handoffできたキーは successor 側から削除（重複を避ける）
            if ok > 0:
                try:
                    del self.contents[key_hex]
                except Exception:
                    pass

        log.info(f"{self.uid} - Handoff to new predecessor {new_pred_uid_hex[:8]} finished: ok={ok}, ng={ng}, range=({old_pred_uid_hex[:8]}, {new_pred_uid_hex[:8]}]")


    
    def notify_new_successor(self, node_dict):
        count_message('notify_new_successor')
        try:
            self.set_successor(node_dict)
            log.info(f"{self.uid} - updated successor to {node_dict.get('ip')}")
        except Exception as e:
            log.warning(f"{self.uid} - failed to update successor: {e}")
        return True

    def fix_fingers(self):
        '''
        ft[i]のエントリを修正(iはランダム)
        '''
        count_message('fix_fingers')
        i = random.randint(1, self.uid.idlength - 1)
        self.fingers[i].set_RespNode(self,self.find_successor(self.fingers[i].key))

    # chord.py 内の LocalNode.init_fingers メソッドの修正

    def init_fingers(self, existingNode:NodeInterface):
        '''
        既存のノードから自身の経路表を設定する。
        '''
        log.info("%s - init_fingers with %s" %(self.uid, existingNode.uid))
        
        # 1. preとsucを設定する
        find_pred_res = existingNode.methodProxy.find_predecessor(self.uid.value)
        
        # ❗️ 修正: RPC失敗時のチェック
        if find_pred_res is None or 'succ' not in find_pred_res:
            log.warning(f"{self.uid} - find_predecessor failed. Falling back to use existingNode as successor.")
            # find_predecessorが失敗した場合、既存のノードを自身のサクセッサとして設定し、
            # プリデセッサは一旦 None または自分自身として処理を継続する
            self.set_successor(existingNode.asdict())
            self.predecessor = None
            
            # ここで処理を終了し、後続の安定化処理（stabilize, fix_fingers）に任せる
            return

        # 2. 正常時: 経路表を設定 (既存のロジック)
        self.set_successor(find_pred_res["succ"]) # predのsucを自身のsucに設定
        self.set_predecessor(find_pred_res)
        
        # ... (後略: 既存の update/FT設定ロジック) ...

    def update_others(self):
        '''自身が参加したことを他ノードに知らせ、ftのupdateを促す
        '''
        count_message('update_others')
        #自身のID-(2^i)のpreにあたるノードは自信をFTに持つ可能性がある
        for i in range(0, self.uid.idlength):
            
            try:
                predNode = self.find_predecessor(self.uid - pow(2, i))
            except Exception as e:
                log.warning("%s - update_others find_predecessor failed i=%d: %r", self.uid, i, e)
                continue

            pred_if, _ = self.get_NodeInterface(predNode)
            #log.debug("%s - try to update FT[%i] of '%s'" %(self.uid, i,predNode.uid))
            res = self._safe_rpc_call(
                pred_if.methodProxy.update_finger_table,
                self.asdict(), i,
                timeout=RPC_PER_CALL_TIMEOUT
            )
            if res is None:
                log.debug("%s - update_others: update_finger_table failed for pred=%s i=%d", self.uid, getattr(pred_if, "uid", "?"), i)

        
    
    def update_finger_table(self, callingNode:dict, i:int):
        count_message('update_finger_table')

        # resp が無いのは異常なのでここだけは弾く
        if i < 0 or i >= len(self.fingers) or self.fingers[i] is None or self.fingers[i].respNode is None:
            log.warning("%s - update_finger_table skipped i=%d (respNode missing)", self.uid, i)
            return True

        # pred が None は join/収束中に普通に起こるので “警告で止めない”
        if self.predecessor is None:
            log.debug("%s - update_finger_table: predecessor is None (still converging) i=%d", self.uid, i)

        try:
            callingNode = BasicNode(callingNode)
            if callingNode.uid == self.uid:
                return True

            resp = self.fingers[i].respNode

            # pred が無くても、この判定と更新はできる
            if callingNode.uid.is_between_l_inclu(self.fingers[i].key, resp.uid):
                log.debug("%s - update FT[%i] with '%s'" %(self.uid, i,callingNode.uid))
                self.fingers[i].set_RespNode(self, callingNode.asdict())

                # pred があるときだけ再帰通知（pred None なら収束後に stabilize に任せる）
                if self.predecessor:
                    try:
                        self.predecessor.methodProxy.update_finger_table(callingNode.asdict(), i)
                    except Exception:
                        pass

            return True
        except Exception as e:
            log.error("%s - update_finger_table fatal error: %s", self.uid, e)
            return True


    def find_successor(self, key):
        count_message('find_successor')
        """指定キーのsucを該当keyのpredのsucを返すことで探す。RPC経由の実行があるため返り値はdict(LocalNode型用)
        predが最近傍じゃないとsucも最近傍じゃない可能性はある
        dict内に何ホップでたどり着いたか'hop':intを含む
        (find_predからもらったホップ数をそのまま返す)
        keyはstr,Key,dictのどれか
        """
        prednode = self.find_predecessor(key)
        if isinstance(key,dict):
            key=Key(key["value"])
        elif isinstance(key,str):
            key=Key(key)
        rslt=prednode["succ"]
        log.debug("%s - found successor for '%s' - node '%s'" %(self.uid, key, rslt['uid'][:9]))
        rslt['hop']=prednode["hop"]
        rslt['rHop']=prednode["rHop"]
        return rslt


    def find_predecessor(self, key):
        count_message('find_predecessor')
        hop = 0
        rHop = 0

        # --- safety guards (avoid hanging RPC) ---
        start_ts = time.time()
        MAX_STEPS = 64
        MAX_SEC = 1.5

        # Key型に変換
        if isinstance(key, dict):
            key = Key(key["value"])
        elif isinstance(key, str):
            key = Key(key)
        if not isinstance(key, Key):
            raise TypeError("find_predecessor arg must be dict, str or Key")

        # ===== ここから：既存コードの「単一ノード or 自身がpred」判定をそのまま残す想定 =====
        # 例: successor が自分、または key が (self, successor] なら self が pred
        # （あなたの既存コード部分をここに残してください）
        # =====

        # 探索開始（既存の else 節に相当）
        closestPredFingerDict = self.closest_preceding_finger(key.value)
        if closestPredFingerDict is None:
            log.warning("%s - closest_preceding_finger returned None for %s. Fallback to self.", self.uid, str(key.value)[:8])
            resdict = self.asdict()
            resdict["succ"] = self.successor.asdict() if getattr(self, "successor", None) else self.asdict()
            resdict["hop"] = hop
            resdict["rHop"] = rHop
            log.debug("%s - found predecessor for '%s' - node '%s'", self.uid, str(key.value)[:8], resdict['uid'][:9])
            increment_exploration_hops(rHop)
            return resdict

        closestPredFinger, hopDiff = self.get_NodeInterface(closestPredFingerDict)  # local
        hop += hopDiff
        rHop += hopDiff

        visited = set()
        steps = 0

        def _uid_str(u):
            try:
                return getattr(u, "value", None) or str(u)
            except Exception:
                return str(u)

        # 初回 successor 取得（None は「不安定/未収束」として break してベストエフォート返す）
        closestPredFingerSuccDict = self._safe_rpc_call(
            closestPredFinger.methodProxy.get_successor,
            timeout=RPC_PER_CALL_TIMEOUT
        )
        if not isinstance(closestPredFingerSuccDict, dict):
            log.warning("%s - get_successor None/invalid from %s during find_predecessor. Fallback best-effort.",
                        self.uid, _uid_str(getattr(closestPredFinger, "uid", "<?>"))[:8])
            resdict = closestPredFinger.asdict() if closestPredFinger else self.asdict()
            # succ は「自分の successor」へ寄せる（少なくともリング内に留まる）
            try:
                resdict["succ"] = self.successor.asdict()
            except Exception:
                resdict["succ"] = self.asdict()
            resdict["hop"] = hop
            resdict["rHop"] = rHop
            log.debug("%s - found predecessor for '%s' - node '%s'", self.uid, str(key.value)[:8], resdict['uid'][:9])
            increment_exploration_hops(rHop)
            return resdict

        closestPredFingerSucc = BasicNode(closestPredFingerSuccDict)

        # ループ（必ず MAX_STEPS / MAX_SEC で脱出）
        while True:
            steps += 1
            if steps > MAX_STEPS or (time.time() - start_ts) > MAX_SEC:
                log.warning("%s - find_predecessor give up (steps/sec) key=%s at=%s succ=%s",
                            self.uid, str(key.value)[:8],
                            _uid_str(getattr(closestPredFinger, "uid", "<?>"))[:8],
                            _uid_str(getattr(closestPredFingerSucc, "uid", "<?>"))[:8])
                break

            cur_id = _uid_str(getattr(closestPredFinger, "uid", "<?>"))
            if cur_id in visited:
                log.warning("%s - find_predecessor loop detected at=%s key=%s",
                            self.uid, str(cur_id)[:8], str(key.value)[:8])
                break
            visited.add(cur_id)

            # 正常終了条件：key が (pred, pred.succ] に入った
            try:
                if key.is_between_r_inclu(closestPredFinger.uid.value, closestPredFingerSucc.uid.value):
                    break
            except Exception:
                # 型が怪しい時も落とさない
                break

            # 次へ：closest_preceding_finger を RPC で取得
            nxtDict = self._safe_rpc_call(
                closestPredFinger.methodProxy.closest_preceding_finger,
                key.value,
                timeout=RPC_PER_CALL_TIMEOUT
            )
            # RPC 1回ぶんの hop/rHop 加算（厳密さより「止まらない」を優先）
            hop += 1
            rHop += 1

            if not isinstance(nxtDict, dict):
                log.warning("%s - closest_preceding_finger None/invalid from %s. Break.",
                            self.uid, str(cur_id)[:8])
                break

            closestPredFinger, hopDiff = self.get_NodeInterface(nxtDict)
            hop += hopDiff
            rHop += hopDiff

            # successor 更新
            sd = self._safe_rpc_call(
                closestPredFinger.methodProxy.get_successor,
                timeout=RPC_PER_CALL_TIMEOUT
            )
            hop += 1
            rHop += 1
            if not isinstance(sd, dict):
                log.warning("%s - get_successor None/invalid after update. Break.", self.uid)
                break
            closestPredFingerSucc = BasicNode(sd)

        # ループ脱出後の best-effort
        resdict = closestPredFinger.asdict() if closestPredFinger else self.asdict()
        try:
            resdict["succ"] = closestPredFingerSucc.asdict()
        except Exception:
            try:
                resdict["succ"] = self.successor.asdict()
            except Exception:
                resdict["succ"] = self.asdict()

        resdict["hop"] = hop
        resdict["rHop"] = rHop
        log.debug("%s - found predecessor for '%s' - node '%s'", self.uid, str(key.value)[:8], resdict['uid'][:9])
        increment_exploration_hops(rHop)
        return resdict


    def closest_preceding_finger(self, keyvalue):
        """keyの最近傍preをFTから探す。他サーバには探しに行かない
        keyvalue == self.uidならself.predecessorを返す
        Iterates reversly on fingers to find the closest known one.

        Args:
            keyvalue (Key,str): 

        Returns:
            dict: selfの時以外はsuccがない

        """
        #自分自身か確認
        # --- normalize keyvalue to hex string ---
        try:
            if isinstance(keyvalue, Key):
                khex = keyvalue.value
            elif isinstance(keyvalue, dict):
                khex = str(keyvalue.get("value") or keyvalue.get("uid") or "")
            else:
                khex = str(getattr(keyvalue, "value", None) or keyvalue)
        except Exception:
            khex = str(keyvalue)

        # key==self のときは predecessor を返す（存在しないなら self）
        try:
            if khex == self.uid.value:
                return (self.predecessor.asdict() if self.predecessor else self.asdict())
        except Exception:
            pass

        for i in range(self.uid.idlength - 1, -1, -1):
            if self.fingers[i].respNode.uid == self.uid:
                if self.successor.uid == self.uid:
                    return self.asdict() #self is alone on the ring
                if Key(khex).is_between_r_inclu(self.uid.value, self.successor.uid.value):
                    return self.asdict()
                continue
            # finger が (self, key) の間にあるならそれを返す
            try:
                f_uid = self.fingers[i].respNode.uid.value
            except Exception:
                continue
            if Key(f_uid).is_between_exclu(self.uid.value, khex):
                return self.fingers[i].respNode.asdict()
        return self.asdict()

    
    
    def _safe_rpc_call(self, method, *args, timeout=RPC_PER_CALL_TIMEOUT, **method_kwargs):
        global _ALREADY_SHUT_WARNED
        # Backward-compat: if caller mistakenly passes timeout as a method kw, don't forward it.
        if 'timeout' in method_kwargs:
            timeout = method_kwargs.pop('timeout')
        # 連打防止
        if not hasattr(self, "_already_shutdown_warned"):
            self._already_shutdown_warned = False
        """指定されたメソッドをRPCで安全に呼び出す。失敗時はNoneを返す。"""
        # submit の直前
        if getattr(_RPC_EXECUTOR, "_shutdown", False):
            # プロセス終了中ならそのまま落とす。そうでなければ復活させる
            if not _ALREADY_SHUT_WARNED:
                _ALREADY_SHUT_WARNED = True
                log.warning("[EXECUTOR_ALREADY_SHUTDOWN] pid=%s thread=%s shutting=%s\n%s",
                            os.getpid(), threading.current_thread().name, _RPC_SHUTTING_DOWN,
                            "".join(traceback.format_stack(limit=25)))
                return None
            _ensure_rpc_executor()
            # それでも shutdown のままなら諦める
            if getattr(_RPC_EXECUTOR, "_shutdown", False):
                log.warning("[EXECUTOR_STILL_SHUTDOWN] pid=%s thread=%s\n%s",
                            os.getpid(), threading.current_thread().name,
                            "".join(traceback.format_stack(limit=25)))
                return None

        try:
            future = _RPC_EXECUTOR.submit(method, *args)
            return future.result(timeout=timeout)

        except (concurrent.futures.TimeoutError,
                xmlrpc.client.Fault,
                xmlrpc.client.ProtocolError,
                ConnectionRefusedError) as e:
            log.debug(f"Safe RPC call failed ({_rpc_method_desc(method)}): {e}")
            return None

        except RuntimeError as e:
            # Executor shutdown の場合など
            log.debug(f"Safe RPC call runtime error ({_rpc_method_desc(method)}): {e}")
            return None

        except Exception as e:
            log.warning(f"Unexpected error in safe RPC call ({_rpc_method_desc(method)}): {e!r}")
            return None


    def _rpc_call_with_fallback(self, start_node_if, method_name, args=(), *,
                                per_call_timeout=RPC_PER_CALL_TIMEOUT,
                                overall_timeout=ROUTING_OVERALL_TIMEOUT,
                                retries=RPC_RETRIES):
        """
        start_node_if (NodeInterface) に対して method_name を args で呼び出す。失敗したら
        successor を辿ってフォールバックする。self.get_NodeInterface を使って
        successor dict -> NodeInterface へ変換する。全体で overall_timeout 秒で打ち切る。
        戻り値: 呼び出しの戻り値
        例: self._rpc_call_with_fallback(succNode, 'return_contents', (key, hop, rHop))
        """
        start_time = time.time()
        current_if = start_node_if

        last_exc = None
        while time.time() - start_time < overall_timeout:
            try:
                method = getattr(current_if.methodProxy, method_name)
                # 短いリトライループ（同一ノードへの再試行）
                attempt_exc = None
                for attempt in range(retries):
                    try:
                        ret = self._safe_rpc_call(method, *args, timeout=per_call_timeout)
                        if ret is None:
                            log.warning(f"_safe_rpc_call returned None: method={method}")
                            raise ValueError("RPC returned None")
                        return ret
                    except Exception as e:
                        attempt_exc = e
                        time.sleep(0.05 * (attempt + 1))
                        continue
                # all retries on this node failed -> will fallthrough to successor handling
                last_exc = attempt_exc
            except Exception as e:
                last_exc = e

            # try to obtain successor of current_if and continue
            try:
                # safe call to get_successor
                succ_dict = self._safe_rpc_call(current_if.methodProxy.get_successor, timeout=per_call_timeout)
                if succ_dict is None:
                    raise ValueError("get_successor returned None")
                # convert succ_dict to NodeInterface; self.get_NodeInterface is local and fast
                current_if, hopdiff = self.get_NodeInterface(succ_dict)
                # continue loop and try next node
            except Exception as e:
                # cannot get successor -> mark last exception and break (no further progress)
                last_exc = e
                break

        # overall timeout or no more successors reachable
        raise TimeoutError(f"RPC '{method_name}' failed with fallback; last_exc={last_exc}")
    # ===== end helper =====

    
    def find_successor_gft(self, key):
        """！未実装！指定キーのsucを該当keyのpredのsucを返すことで探す。使用するFTはGFT．RPC経由の実行があるため返り値はdict(LocalNode型用)
        predが最近傍じゃないとsucも最近傍じゃない可能性はある
        dict内に何ホップでたどり着いたか'hop':intを含む
        (find_predからもらったホップ数をそのまま返す)
        keyはstr,Key,dictのどれか
        """
        prednode = self.find_predecessor_gft(key)
        if isinstance(key,dict):
            key=Key(key["value"])
        elif isinstance(key,str):
            key=Key(key)
        rslt=prednode["succ"]
        log.debug("%s - found successor for '%s' - node '%s'" %(self.uid, key, rslt['uid'][:9]))
        rslt['hop']=prednode["hop"]
        rslt['rHop']=prednode["rHop"]
        return rslt

    def find_predecessor_gft(self, key):
        """！未実装！指定キーのpredをRPCを繰り返して探す。使用するFTはGFT．RPC経由の実行があるため返り値はdict(LocalNode型用)
        keyはstr,Key,dictのどれか
        状況によっては最近傍じゃないpredが返る可能性がある。
        dict内に何ホップでたどり着いたか'hop':intを含む(このメソッド中に何回サーバ間通信したか)
        rHop:get_NI+methodproxyされた回数, hop:chord理論上必要な最低限(=クライアントから該当サーバにたどり着くまで何サーバ経由したか)
        """
        hop=0 #hop数
        rHop=0
        #Key型に変換
        if isinstance(key, dict):
            key = key["value"]
            key = Key(key)
        elif isinstance(key, str):
            key = Key(key)
        if not isinstance(key, Key):
            raise TypeError("find_predecessor arg must be dict, str or Key")

        #log.debug("%s - find_predecessor for '%s'" %(self.uid, key.value))
        #自分自身を返す
        if self.uid == self.successor.uid: #NWに自分しかいない（単一ノードNW）
            resdict=self.asdict()
        elif key.is_between_r_inclu(self.uid, self.successor.uid): #自身のsucの管理なら自分がpred
            resdict=self.asdict()
        #TODO IDEA maybe: overwrite dispatch on xmlrpc server
        # then it is possible to dispatch on specific method for rpc
        # so in the next line case we are not force to transform closestPredFinger into a NodeInterface
        #TODO avoid casting directly in NodeInterface because we loose potential succ info from the original dict
        #探索開始。
        else:
            closestPredFinger,hopDiff= self.get_NodeInterface(self.closest_preceding_finger(key.value)) #前で自分自身を省いているので必ずRPC.自身のFTからkeyのpred候補のNodeInterfaceを取得。以下closestPredFingerのmethodProxyではrHopを１プラスする
            rHop+=hopDiff
            hop+=hopDiff
            closestPredFingerSucc = BasicNode(closestPredFinger.methodProxy.get_successor()) #closestPredFingerのsucを取得。RPCしないのでBasicNode型。
            rHop+=hopDiff
            if closestPredFinger.uid == closestPredFingerSucc.uid: #各サーバが異なるIDを持つ要件を満たしていないor　sucに自分が入っている。ループしちゃうのでここで処理を打ち切る。
                #FTは修正しない。同IDでも異なるサーバの可能性があるので一応そのまま返す。
                resdict = closestPredFinger.asdict()
                resdict["succ"] = closestPredFingerSucc.asdict() #本来BasicNodeにはない["succ"]の項目を足して実質LocalNodeにしてる
            else:
                #closestPredFinger.uid<key<=closestPredFingerSucc.uidになるまでclosestPredFingerのFTからclosestPredFingerを更新
                while not key.is_between_r_inclu(closestPredFinger.uid, closestPredFingerSucc.uid):
                    closestPredFingerDict = closestPredFinger.methodProxy.closest_preceding_finger(key.value)
                    rHop+=hopDiff
                    #↓元はif文とelseだった
                    closestPredFinger,hopDiff = self.get_NodeInterface(closestPredFingerDict) #更新
                    hop+=hopDiff
                    rHop+=hopDiff
                    closestPredFingerSucc = BasicNode(closestPredFinger.methodProxy.get_successor())
                    rHop+=hopDiff
                    #↑ここまで
                resdict = closestPredFinger.asdict()
                resdict["succ"] = closestPredFingerSucc.asdict() #本来BasicNodeにはない["succ"]の項目を足して実質LocalNodeにしてる
        resdict['hop']=hop #最終的なホップ数を足す
        resdict['rHop']=rHop
        log.debug(f"{self.uid} - found predecessor for '{key}' - node '{resdict['uid'][:9]}'")
        return resdict

    def closest_preceding_finger_gft(self, keyvalue):
        """！未実装！keyの最近傍preをGFT(exFingers)から探す。他サーバには探しに行かない．
        keyvalue == self.uidならself.predecessorを返す
        Iterates reversly on fingers to find the closest known one.

        Args:
            keyvalue (Key,str): 

        Returns:
            dict: selfの時以外はsuccがない

        """
        #自分自身か確認
        if self.uid == keyvalue:
            return self.predecessor.asdict()
        for i in range(self.uid.idlength - 1, -1, -1):#すべての要素をとおいほうから走査
            if self.exFingers[i].respNode.uid == self.uid:
                #self is alone on the ring
                if self.successor.uid == self.uid:
                    return self.asdict()
                if Key(keyvalue).is_between_r_inclu(self.uid, self.successor.uid):
                    return self.asdict()
                continue
            if self.exFingers[i].respNode.uid.is_between_exclu(self.uid, keyvalue):
                return self.fingers[i].respNode.asdict()
        return self.asdict()

    def updatefinger(self, firstnode):
        '''全ノードのFTを全て再構築する。RPCを大量発生させるためコスト高い。
        (全体的に)壊れたFTを直すためなのでsucしか使わない。
        Update finger table
        Dummy update which loookup all fingerkey
        When finished, propagate updatefinger to all node of the ring
        Very costly in rpc
        finger is an array of dict {resp, key}
            `resp` is the Node responsible for `key`
        ・ firstnode: 最初に updatefinger を呼び出したノード
        '''
        #FTの
        for i in range(0, self.uid.idlength):
            resp = self.lookupWithSucc(self.fingers[i].key) #辞書型
            self.fingers[i].set_RespNode(self,resp)
        #firstnodeに辿り着くまでupdatefingerする
        if firstnode.uid != self.fingers[0].respNode.uid:
            self.fingers[0].respNode.methodProxy.updatefinger(firstnode)

    def lookupWithSucc(self, key):
        """sucのみで探索する最も原始的なchordルーティング。Updatefinger以外で使ってない。
        Lookup method which uses only successor information
        Provided a key, lookupWithSucc return a dict with basic info
        about the responsible node of the provided key

        ・ key: key to look for the responsible
        """
        #Key型に統一
        if isinstance(key, Key):
            keyLookedUp = key
        elif isinstance(key, str):
            keyLookedUp = Key(key)
        else:
            raise TypeError

        # Self is successor ?
        if self.uid == keyLookedUp:
            return {"ip": self.ip, "port": self.port}
        # Is self.successor the successor of key ?
        if keyLookedUp.isbetween(self.uid.value, self.successor.uid.value):
            return {"ip": self.successor.ip, "port": self.successor.port}

        return self.successor.methodProxy.lookupWithSucc(key)

    def calcfinger(self, k):
        '''
        自身のキーから2^k離れたキーを返す
        Returns computed key for finger k
        ・ k: from 0 to (m - 1)
        '''
        #範囲外(kはm未満のはず)のときはエラー
        if k > self.uid.idlength - 1: #idlength=256
            raise ValueError("calcfinger: value above {} are not accepted".format(self.idlength))
        return self.uid + pow(2, k)

    def printFingers(self):
        '''
        現在の finger table を出力してデバッグ・確認を行う。
        '''
        for n, f in enumerate(self.fingers):
            print("TABLE: finger{0} : "
                "- key: {2} - resp: {1}"
                .format(n, f.respNode.uid, f.key))
    
    # ---- Debug RPC (non-invasive): diagnose why get_content returned None ----
    def debug_get_content_status(self, key):
        """Return {'status': 'hit'|'miss'|'rpc_error', 'step': ...} without changing normal behavior."""
        info = {"key": key}
        try:
            fs = self.find_successor(key)
            if fs is None:
                info.update({"status": "rpc_error", "step": "find_successor", "detail": "returned None"})
                return info
            node_if, hop = fs
            info.update({"responsible_uid": getattr(node_if, "uid", None), "hop": hop})
            if node_if is None:
                info.update({"status": "rpc_error", "step": "find_successor", "detail": "node_if is None"})
                return info

            # Local check if responsible is self
            if getattr(node_if, "uid", None) == self.uid:
                with self.contents_lock:
                    v = self.contents.get(key)
                info.update({"status": "hit" if v is not None else "miss", "step": "local"})
                return info

            # Ask responsible node directly (no fallbacks here)
            out = None
            try:
                out = node_if.methodProxy.return_contents([key])
            except Exception as e:
                info.update({"status": "rpc_error", "step": "return_contents", "detail": repr(e)})
                return info

            if isinstance(out, list) and len(out) == 1 and out[0] is not None:
                info.update({"status": "hit", "step": "return_contents"})
                return info
            info.update({"status": "miss", "step": "return_contents", "returned_type": str(type(out))})
            return info
        except Exception as e:
            info.update({"status": "rpc_error", "step": "exception", "detail": repr(e)})
            return info

    def debug_local_contents_size(self):
        log.warning(f"{self.uid} - debug_local_contents_size called; contents_len={len(getattr(self,'contents',{}))}")
        try:
            with self.contents_lock:
                return int(len(self.contents))
        except Exception:
            return -1

class Finger:
    '''
    Finger Table用クラス。FTにおける1つのエントリ（行）を表す
    Attrs:
    ・ key(key): このエントリのキー(respノードのkeyではない。FT[i]=self.uid+2^i)
    ・ respNode(NodeInterface): key以上で、keyのsucにあたるkeyを持つサーバ。要は転送先
    '''
    def __init__(self, key, originNode,respNode):
        #keyがstrならKey型に変える
        if isinstance(key, str):
            self.key = Key(key)
        elif isinstance(key, Key): #key型かuid型ならOK
            self.key = key
        else:
            raise TypeError("key type not accepted. Support str and Key")

        #self.respNodeを設定。dict or NodeInterfaceを受け付ける。最終的にNodeInteface型になる。
        self.set_RespNode(originNode,respNode)

    def set_RespNode(self, originNode:LocalNode,respNode:NodeInterface):
        '''
        respnodeはdictとNodeInterface型を受け付ける
        originNodeはbootstarapノードのようなもの
        '''
        if isinstance(respNode, dict):
            self.respNode,hop = originNode.get_NodeInterface(respNode)
        elif isinstance(respNode, NodeInterface):
            self.respNode = respNode
        else:
            raise TypeError(f"Finger.set_RespNode() accept dict or NodeInterface. Not {type(respNode)}")


class Contents():
    '''
    具体的なコンテンツ（グラフノード）。
    
    Attrs:
        data:データの実体。任意の型
        id:ノードID
        uid(key):key。initの引数がkey型or do_hash==Falseでそのまま登録。strかつTrueでhash化
        gid:所属グラフのID。任意の型
        neighbors(list):隣接ノードのリスト。defaultはなし。[Contents,Contents,...]。辞書型かcontents型のリストを受け取る
        
    '''
    def __init__(self,gid,id,data,uid,do_hash=False,neighbors=None):
        self.id=id
        self.data=data
        self.gid=gid
        if isinstance(uid,Key):
            self.uid=uid
        elif isinstance(uid,str):
            if do_hash:
                self.uid=Uid(uid) #コンテンツをハッシュ化したものをキーとする
            else:
                self.uid=Key(uid) #ハッシュ化はしない
        else:
            raise TypeError("uid must be Key or str")
        if neighbors: #隣接ノードが存在
            if isinstance(neighbors[0],dict):
                self.neighbors=[Contents(c['gid'],c['id'],c['data'],c['uid'],neighbors=c['neighbors']) for c in neighbors]
            elif isinstance(neighbors[0],Contents):
                self.neighbors=neighbors
            else:
                raise TypeError("Supports Contents or dict")
        else:
            self.neighbors=None
    
    def set_neighbors(self,neighbors:list):
        '''
        コンテンツ型or辞書型のリストを渡してneighborsを登録・更新
        '''
        if isinstance(neighbors[0],dict):
            self.neighbors=[Contents(c['gid'],c['id'],c['data'],c['uid']) for c in neighbors]
        elif isinstance(neighbors[0],Contents):
            self.neighbors=neighbors
        else:
            raise TypeError("Supports Contents or dict")
    
    def asdict(self,withNeighbors=True,recursion=0,adj=3):
        """主にRPC用にcontentsの情報を返す。
        RPCの引数＆返り値はプリミティブ型のみなのでdictが返り値。またdictの鍵は文字列じゃないとダメ。
        
        Args:
            adj:neighborsの再帰実行回数＝隣接nノードまでのdict情報を含む。デフォルトは想定最大値の3．0なら追加情報なし
        
        Returns:
            dict: uidはkey型からstrに変換。neighborsはあればdictのlist
        """
        r={'gid': self.gid,
           'id': self.id,
            "data": self.data,
            "uid": self.uid.value}
        if withNeighbors and self.neighbors and recursion<adj:
            r['neighbors']=[c.asdict(recursion=recursion+1,adj=adj) for c in self.neighbors]
        else:
            r['neighbors']=None
        return r



if __name__ == '__main__':
    log.warning("[BOOT] chord.py=%s pid=%s", __file__, os.getpid())
    server = LocalNode(sys.argv[1], 17000, _stabilizer=True)

    if sys.argv[2] == '1':
        bootNode = BasicNode({'ip': '172.16.1.11', 'port': 17000})
        log.info("%s - Join Chord ring through %s" % (server.uid, bootNode.uid))
        server.join(server.get_NodeInterface(bootNode.asdict())[0])
    else:
        log.info("%s - This is the first Node" % (server.uid))

    # ここが重要：main を終了させない
    try:
        while not server._stop_event.is_set():
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        set_rpc_shutting_down("main.finally")
        try:
            server.stop()
        finally:
            shutdown_rpc_executor("main.finally")
