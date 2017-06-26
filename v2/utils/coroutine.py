import functools


def Coroutine(func):
    @functools.wraps
    def wrapper(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.__next__()
        return cr
    return wrapper
