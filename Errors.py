# Custom Exception to Catch the Records that fail checks
class CheckException(AttributeError):
    pass


class PrevNotFound(KeyError):
    pass


class PrevTooOld(Exception):
    pass


class RefreshError(Exception):
    pass
