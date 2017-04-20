import sys
is_py2 = sys.version[0] == '2'
import threading
if is_py2:
    import Queue as queue
else:
    import queue as queue
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count


class EventDriver(object):
    def __init__(self, maxtask=0):
        self.__eventQueue = queue.Queue(maxsize=maxtask)
        self.__active = False
        self.__thread = threading.Thread(target=self.__run)
        self.__handlers = {}
        self.__execute_pool = ThreadPoolExecutor(max_workers=2*cpu_count())

    def __run(self):
        while self.__active:
            try:
                event = self.__eventQueue.get(block=True)
                if event._type == 'EXIT':
                    break
                self.process(event)
                # self.__execute_pool.submit(self.process, event)
            except queue.Empty:
                pass

    def process(self, event):
        if event._type in self.__handlers:
            for handler in self.__handlers[event._type]:
                handler(event)

    def start(self, Daemon=False):
        self.__active = True
        self.__thread.setDaemon(Daemon)
        self.__thread.start()

    def stop_now(self):
        self.__active = False
        self.__execute_pool.shutdown()
        self.__thread.join(timeout=5)

    def stop_util_complete(self):
        self.send_event(TheEvent(type='EXIT'))
        # self.__execute_pool.shutdown()
        self.__thread.join()

    def add_event_listener(self, event_type, handler):
        try:
            handler_list = self.__handlers[event_type]
        except KeyError:
            handler_list = []
        if handler not in handler_list:
            handler_list.append(handler)
        self.__handlers[event_type] = handler_list

    def send_event(self, event):
        self.__eventQueue.put(event)

class TheEvent(object):
    def __init__(self, type=None, dict={}):
        self._type = type
        self._dict = dict