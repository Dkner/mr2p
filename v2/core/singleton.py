try:
    from synchronize import make_synchronized
except ImportError:
    def make_synchronized(func):
        import threading
        func.__lock__ = threading.Lock()

        def synced_func(*args, **kws):
            with func.__lock__:
                return func(*args, **kws)

        return synced_func


class Singleton(object):
    instance = None

    @make_synchronized
    def __new__(cls, *args, **kw):
        if cls.instance is None:
            cls.instance = object.__new__(cls, *args, **kw)
        return cls.instance


class Factory(object):
    pass