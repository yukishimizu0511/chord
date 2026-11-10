from threading import Thread, Event
import time
import logging

log = logging.getLogger('chordLog')

class Stabilizer(object):
    '''
    スタビライズ用のクラス。ローカルノード型が引数
    '''
    def __init__(self, local_node):
        self.node = local_node
        self.stop_event = Event()
        self.th = Thread(
            target=Stabilizer.stabilizer_loop,
            args=(self, self.stop_event, self.node),
            name=f"Stabilizer-{getattr(local_node,'ip', 'unknown')}"
        )
        self.th.daemon = False

    def stop(self, timeout=5.0):
        self.stop_event.set()
        if self.th.is_alive():
            log.info("Waiting for Stabilizer thread to finish...")
            self.th.join(timeout)
            if self.th.is_alive():
                log.warning("Stabilizer thread did not exit after join timeout")

    def start(self):
        self.th.start()

    def stabilizer_loop(self, stop_event, node, secs=5):
        """
        @param secs: time, in second, to repeat the loop
        """
        while not stop_event.is_set():
            try:
                node.stabilize()
                node.fix_fingers()
            except Exception as e:
                log.exception(f"Exception in stabilizer loop: {e}")
            # stop_eventがsetされたらループを抜ける
            stop_event.wait(secs)
            
