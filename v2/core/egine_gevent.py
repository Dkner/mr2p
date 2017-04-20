import sys
is_py2 = sys.version[0] == '2'
import threading
if is_py2:
    import Queue as queue
else:
    import queue as queue
import gevent.monkey
gevent.monkey.patch_socket()
import gevent

class EventDriver(object):
    def __init__(self, maxtask=0):
        self.__eventQueue = queue.Queue(maxsize=maxtask)
        self.__active = False
        self.__queue_thread = threading.Thread(target=self.__run)
        self.__handlers = {}

    def __run(self):
        while self.__active:
            try:
                event = self.__eventQueue.get(block=True)
                coroutine = gevent.spawn(self.process, event)
                gevent.joinall(coroutine)
            except queue.Empty:
                pass

    def process(self, event):
        if event._type in self.__handlers:
            for handler in self.__handlers[event._type]:
                handler(event)

    def start(self, Daemon=False):
        self.__active = True
        self.__queue_thread.setDaemon(Daemon)
        self.__queue_thread.start()

    def stop_now(self):
        self.__active = False

    def stop_util_complete(self):
        self.__active = False

    def join(self):
        self.__queue_thread.join(5)

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
