import hashlib


class Tool:
    @staticmethod
    def md5(string):
        if isinstance(string, str):
            string = string.encode()
        m = hashlib.md5()
        m.update(string)
        return m.hexdigest()