# Patched clientxmlrpc.py
import xmlrpc.client
import socket
import http.client
import logging, traceback, threading
from typing import Optional

log = logging.getLogger('chordLog')

# Default timeouts (seconds)
DEFAULT_CONNECTION_TIMEOUT = 5  # when connecting to a node that may have left
DEFAULT_RPC_TIMEOUT = 5       # for longer routing operations if needed

def _where(msg: str):
    log.warning("[EXEC_TRACE] %s\n%s", msg, "".join(traceback.format_stack(limit=12)))

class TimeoutTransport(xmlrpc.client.Transport):
    """Transport that creates HTTPConnection objects with a socket timeout.

    This ensures RPC calls will raise socket.timeout after `timeout` seconds
    instead of blocking forever.
    """
    def __init__(self, timeout: Optional[float] = None, use_datetime=False):
        super().__init__(use_datetime=use_datetime)
        self._timeout = timeout

    def make_connection(self, host):
        # host is "host:port" or "host"
        # Create a HTTPConnection with timeout set so socket operations time out.
        h = host
        # xmlrpc.client.Transport normally uses http.client.HTTPConnection
        # We'll create one with a timeout. For simplicity we do not support
        # HTTPS here (not needed by chord nodes).
        return http.client.HTTPConnection(h, timeout=self._timeout)


class ChordClientxmlrpcProxy(xmlrpc.client.ServerProxy):
    """
    RPCクライアント用クラス。ServerProxyのサブクラス。
    Added: configurable transport timeout + safe-call wrapper to avoid
    blocking when peers are down.
    """
    def __init__(self, ip: str, port: int, timeout: Optional[float] = DEFAULT_CONNECTION_TIMEOUT):
        # Build URL as before
        url = "http://{ip}:{port}/chord".format(ip=ip, port=port)
        transport = TimeoutTransport(timeout=timeout)
        # ServerProxy constructor expects a host URL and optional transport
        super().__init__(url, allow_none=True, transport=transport)
        self._conn_timeout = timeout

    def getXmlRPCClient(self, ip=None, port=None, node=None, timeout: Optional[float] = None):
        """
        Create and return a ChordClientxmlrpcProxy

        Backwards compatible with the original signature. Adds an optional
        `timeout` argument (in seconds) to control socket timeouts for the
        returned proxy. If timeout is None the default connection timeout is used.
        """
        if ip == port == node == None or node == ip == None or node == port == None:
            raise RuntimeError("Provide either ip and port or node")

        if timeout is None:
            timeout = DEFAULT_CONNECTION_TIMEOUT

        if isinstance(ip, str) and isinstance(port, int):
            return ChordClientxmlrpcProxy(ip, port, timeout=timeout)
        elif isinstance(node, chord.BasicNode):
            return ChordClientxmlrpcProxy(node.ip, node.port, timeout=timeout)
        else:
            raise TypeError

    # Wrap attribute access so that method calls are caught and converted
    # to clear exceptions instead of hanging the caller.
    def __getattr__(self, name):
        # xmlrpc.client.ServerProxy provides a _Method object when accessed.
        # We return a wrapper that calls the underlying RPC and catches
        # common network errors, converting them into python exceptions the
        # caller can handle (or returns None).
        try:
            method = super().__getattr__(name)
        except AttributeError:
            # If the underlying ServerProxy doesn't provide the attribute,
            # return a callable that raises AttributeError when invoked.
            def missing(*args, **kwargs):
                raise AttributeError(f"RPC method {name} not found on proxy {self._ServerProxy__host}")
            return missing

        def wrapped(*args, _raise_on_error: bool = False, **kwargs):
            if '_raise_on_error' in kwargs:
                kwargs.pop('_raise_on_error')
            # Do not forward transport-level timeouts to XML-RPC methods
            kwargs.pop('timeout', None)
            kwargs.pop('call_timeout', None)
                
            try:
                return method(*args, **kwargs)
            except xmlrpc.client.Fault as e:
                # Remote raised an exception -- bubble up (caller may want it)
                log.debug(f"XML-RPC Fault calling {name}: {e}")
                raise
            except xmlrpc.client.ProtocolError as e:
                log.warning(f"ProtocolError calling {name} on {self._ServerProxy__host}: {e}")
                if _raise_on_error:
                    raise
                return None
            except socket.timeout as e:
                log.warning(f"socket.timeout calling {name} on {self._ServerProxy__host}: {e}")
                if _raise_on_error:
                    raise
                return None
            except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError, OSError) as e:
                # Covers connection refused, reset by peer, DNS failures, etc.
                #log.warning(f"Network error calling {name} on {self._ServerProxy__host}: {e}")
                if _raise_on_error:
                    raise
                return None

        return wrapped

    def close(self):
        """Close underlying transport connections if possible."""
        # ServerProxy may implement close in newer Python versions
        try:
            super().close()
        except Exception:
            # ignore if not supported
            pass


# Factory / adapter to be used by chord.NodeInterface
class RPCProxyAdapter:
    """
    Adapter that wraps a ChordClientxmlrpcProxy and guarantees that
    attribute access always returns a callable (so chord.py can do
    proxy.some_method(...)) and that network errors are reported as None
    rather than hanging.
    """
    def __init__(self, ip: str, port: int, timeout: Optional[float] = DEFAULT_CONNECTION_TIMEOUT):
        try:
            self._inner = ChordClientxmlrpcProxy(ip, port, timeout=timeout)
        except Exception as e:
            log.warning(f"Failed to create ChordClientxmlrpcProxy for {ip}:{port}: {e}")
            self._inner = None

    def __getattr__(self, name):
        # When inner proxy missing, return a dummy callable that returns None
        if self._inner is None:
            def dummy(*args, **kwargs):
                return None
            return dummy
        # Otherwise delegate to inner proxy but ensure we always return a callable
        try:
            return getattr(self._inner, name)
        except AttributeError:
            def missing(*args, **kwargs):
                # method not supported
                return None
            return missing

    def close(self):
        if self._inner:
            try:
                self._inner.close()
            except Exception:
                pass


def create_proxy(ip: str, port: int, timeout: Optional[float] = DEFAULT_CONNECTION_TIMEOUT) -> RPCProxyAdapter:
    """Convenience factory used by chord.NodeInterface to get a safe proxy."""
    return RPCProxyAdapter(ip, port, timeout=timeout)


# Helper for callers who want a safe RPC call with explicit timeouts

def safe_rpc_call(proxy: ChordClientxmlrpcProxy, method_name: str, *args, timeout: Optional[float] = None, raise_on_error: bool = False, **kwargs):
    """
    Call method_name on proxy with network errors turned into None (or raised
    when raise_on_error=True). `timeout` is reserved for future use (e.g. to
    create a per-call transport) but for now callers should create a proxy
    with the desired socket timeout.
    """
    method = getattr(proxy, method_name)
    return method(*args, _raise_on_error=raise_on_error, **kwargs)


# -------------------------------------------------------------
# Patched excerpt of chord.py (only the modified portion shown)
# We change NodeInterface.__init__ to use clientxmlrpc.create_proxy so
# methodProxy always behaves like a callable-bearing adapter and will
# return None on network errors instead of causing NoneType callables.
# -------------------------------------------------------------

# --- patch start: NodeInterface __init__ snippet ---
# (This is intended to replace the corresponding block in your chord.py)

class NodeInterface:
    """
    BasicNodeを継承。各サーバをコピーして仮想サーバを作り、仮想サーバ経由でメソッドを実行することで、他サーバのメソッド使用時にいちいち通信用コードを書く必要がなくなる。
    """
    def __init__(self, arg):
        import chord
        # 仮想ノードの作成
        if isinstance(arg, chord.LocalNode): #自サーバならそのまま実行
            self.node = arg #該当サーバの情報をコピー
            self.methodProxy = arg #メソッドを実行する仮想サーバにはそのまま該当サーバの参照を入れれば良い。
        elif isinstance(arg, dict): #他サーバならRPC
            self.node = chord.BasicNode(arg["ip"], arg["port"]) #{ip:val,port:val}という形式なのでそのまま渡せば良い
            # メソッドはRPC - create_proxy を使って安全なプロキシを返す
            try:
                # chord.clientxmlrpc.create_proxy を使うため、clientxmlrpc がモジュールとして
                # import されていることを期待します（chord.py 内では既に clientxmlrpc を import しているはず）。
                self.methodProxy = clientxmlrpc.create_proxy(self.ip, int(self.port), timeout=DEFAULT_CONNECTION_TIMEOUT)
            except Exception as e:
                # フォールバック: 何か失敗したときは dummy proxy を入れて None を返すようにする
                log.warning(f"Failed to create RPC proxy for {self.ip}:{self.port}: {e}")
                class _DummyProxy:
                    def __getattr__(self, name):
                        def _dummy(*args, **kwargs):
                            return None
                        return _dummy
                self.methodProxy = _DummyProxy()
        else:
            raise TypeError("Supports LocalNode or dict")

# --- patch end ---


# Note: Please copy the above NodeInterface.__init__ changes into your chord.py
# replacing the original implementation. The clientxmlrpc.create_proxy factory
# provides a resilient adapter that guarantees attribute access returns a
# callable; network errors will produce None results instead of causing
# non-callable attributes.
