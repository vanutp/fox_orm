class OrmException(Exception):
    pass


class AlreadyInitializedException(OrmException):
    pass


class WTFException(OrmException):
    pass


class NotFetchedException(OrmException):
    pass


__all__ = ['OrmException', 'AlreadyInitializedException', 'WTFException', 'NotFetchedException']
