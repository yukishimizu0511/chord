import logging
from collections import defaultdict
import socket
import traceback
import threading
import time
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    daemon_threads = True          # request thread をプロセス終了に巻き込む
    allow_reuse_address = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._active_lock = threading.Lock()
        self._active = 0
        self._closing = threading.Event()

    def process_request_thread(self, request, client_address):
        with self._active_lock:
            self._active += 1
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._active_lock:
                self._active -= 1

    def active_requests(self) -> int:
        with self._active_lock:
            return self._active

    def mark_closing(self):
        self._closing.set()

    def is_closing(self) -> bool:
        return self._closing.is_set()

log = logging.getLogger('chordLog') # chordLogロガーを取得 (chord.pyで設定されているものと同じ)

# メッセージ数のカウンター
MESSAGE_COUNTS = {
    'stabilize': 0,
    'fix_fingers': 0,
    'get_predecessor': 0,
    'notify_new_predecessor': 0,
    'get_successor': 0,
    'find_successor': 0,
    'find_predecessor': 0,
    'put_content': 0,
    'return_contents': 0,
    'delete_all_cashes': 0,
    'delete_all_contents': 0,
    'set_exFinger_table': 0,
    'get_content': 0,
    'return_contents_ex': 0,
    'return_all_contents': 0,
    'return_FT': 0,
    'return_cash': 0,
    'return_range_of_key': 0,
    'export_contents_in_range': 0,
    'pull_from_successor_after_join': 0,
    'get_NodeInterface': 0,
    'update_finger_table': 0,
    'update_others': 0,
    'stop': 0,
    '__default__': 0 # 未分類のRPCをカウント
}

MESSAGE_COUNTS = defaultdict(int)
_MESSAGE_COUNTS_LOCK = threading.Lock()

GLOBAL_EXPLORATION_HOPS = 0
_GLOBAL_HOPS_LOCK = threading.Lock()

def increment_exploration_hops(hops: int):
    """探索ホップ数を加算（スレッド安全）"""
    global GLOBAL_EXPLORATION_HOPS
    with _GLOBAL_HOPS_LOCK:
        GLOBAL_EXPLORATION_HOPS += hops
    return GLOBAL_EXPLORATION_HOPS

def get_exploration_hops_snapshot():
    """現在の探索ホップ総数を返す"""
    with _GLOBAL_HOPS_LOCK:
        return GLOBAL_EXPLORATION_HOPS

def reset_exploration_hops_global():
    """探索ホップ数をリセット"""
    global GLOBAL_EXPLORATION_HOPS
    with _GLOBAL_HOPS_LOCK:
        GLOBAL_EXPLORATION_HOPS = 0
    return True

def count_message(name: str):
    """メッセージ種別 name をインクリメント（スレッド安全）"""
    with _MESSAGE_COUNTS_LOCK:
        MESSAGE_COUNTS[name] += 1

def get_message_counts_snapshot():
    """現在のカウントを辞書で返す（コピーを返す）"""
    with _MESSAGE_COUNTS_LOCK:
        return dict(MESSAGE_COUNTS)

def reset_message_counts_global():
    """全カウントをリセット（サーバ内ローカル）"""
    with _MESSAGE_COUNTS_LOCK:
        MESSAGE_COUNTS.clear()
    return True

def delete_all_contents_servers(self):
    self.node.delete_all_contents()
    return True

def get_total_message_count_snapshot():
    with _MESSAGE_COUNTS_LOCK:
        return sum(MESSAGE_COUNTS.values())

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/chord', '/RPC2', '/')


class ChordServerxmlrpc:
    def __init__(self, ip, port, instance):
        self.ip = ip
        self.port = port
        self._srv = ThreadedXMLRPCServer((ip, port), requestHandler=RequestHandler, allow_none=True, logRequests=False)
        self._srv.register_instance(instance, allow_dotted_names=True)
        
        # lookup をクライアントから呼ぶため明示公開（安全策）
        try:
            self._srv.register_function(instance.find_successor, "find_successor")
        except Exception:
            pass

        # 切り分け用：get_content を明示登録（instance に存在する前提）
        try:
            self._srv.register_function(instance.get_content, "get_content")
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("register_function(get_content) failed: %s", e)
        try:
            self._srv.register_function(instance.return_contents, "return_contents")
        except Exception:
            pass
        try:
            self._srv.register_function(instance.get_successor, "get_successor")
        except Exception:
            pass
        
        try:
            self._srv.register_function(instance.store_content_primary, "store_content_primary")
        except Exception:
            pass
        
        try:
            self._srv.register_function(instance.get_successor_list, "get_successor_list")
        except Exception:
            pass
        
        try:
            self._srv.register_function(instance.get_transfer_stats, "get_transfer_stats")
        except Exception:
            pass
        
        try:
            self._srv.register_function(instance.repair_pull_from_primary, "repair_pull_from_primary")
        except Exception:
            pass

        try:
            self.server.register_function(
                self.node.get_departed_transfer_stats_snapshot,
                "get_departed_transfer_stats_snapshot"
            )
        except Exception:
            pass
        
        try:
            self._srv.register_function(instance.return_content_one, "return_content_one")
            self._srv.register_function(instance.get_content_from_here, "get_content_from_here")
        except Exception:
            pass


        self._t = threading.Thread(target=self._srv.serve_forever, name=f"XMLRPC-{ip}:{port}", daemon=True)
        self._t.start()

    def start(self):
        # 既に __init__ で起動しているので no-op
        return True

    def serve_forever(self):
        # 互換用（外から呼ばれたら現在スレッドで回す）
        self._srv.serve_forever()

    def shutdown_graceful(self, timeout: float = 2.0):
        # 1) 以降の新規受付を止める（目印）
        self._srv.mark_closing()

        # 2) accept/serve_forever 停止
        try:
            self._srv.shutdown()
        except Exception:
            pass
        try:
            self._srv.server_close()
        except Exception:
            pass

        # 3) 残っている request thread が落ちるのを短時間待つ
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._srv.active_requests() == 0:
                break
            time.sleep(0.05)

    def active_requests(self) -> int:
        return self._srv.active_requests()

    def is_closing(self) -> bool:
        return self._srv.is_closing()

