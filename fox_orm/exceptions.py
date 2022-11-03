class OrmError(Exception):
    pass


class ModelCreationError(OrmError):
    pass


class UnannotatedFieldError(ModelCreationError):
    def __init__(self, field_name: str):
        super().__init__(f'Unannotated field {field_name!r}')


class PrivateColumnError(ModelCreationError):
    def __init__(self, field_name: str):
        super().__init__(f'Column names cannot start with _ (got {field_name!r})')


class NoPrimaryKeyError(ModelCreationError):
    def __init__(self):
        super().__init__('Model must have at least one primary key')


class AbstractModelRelationError(ModelCreationError):
    def __init__(self):
        super().__init__('Abstract models cannot have relations')


class UnsupportedTypeError(ModelCreationError):
    def __init__(self, type_: str, column_name: str):
        super().__init__(
            f'Unsupported type {type_!r} specified in annotation for column {column_name!r}'
        )


class NoTypeError(ModelCreationError):
    def __init__(self, column_name: str):
        super().__init__(f'No type specified for column {column_name!r}')


class MultipleTypesError(ModelCreationError):
    def __init__(self, column_name: str):
        super().__init__(
            f'More than one type specified in arguments for column {column_name!r}'
        )


class AbstractModelInstantiationError(OrmError):
    def __init__(self):
        super().__init__('Abstract models cannot be instantiated')


class InvalidColumnError(OrmError):
    def __init__(self, column_name: str):
        super().__init__(f'Invalid column {column_name!r}')


class UnboundInstanceError(OrmError):
    def __init__(self):
        super().__init__('Instance is not bound to db, call .save first')


class QueryBuiltError(OrmError):
    def __init__(self):
        super().__init__(
            'Query is already built, but you are trying to call a method modifying it'
        )


class InvalidQueryTypeError(TypeError):
    def __init__(self, got: str, expected: str = None):
        text = f'Invalid query type {got!r}'
        if expected is not None:
            text += f', expected {expected!r}'
        super().__init__(text)


class NoSuchColumnError(AttributeError):
    def __init__(self, column_name: str):
        super().__init__(f'No such column: {column_name!r}')


__all__ = [
    'OrmError',
    'ModelCreationError',
    'UnannotatedFieldError',
    'PrivateColumnError',
    'NoPrimaryKeyError',
    'AbstractModelRelationError',
    'UnsupportedTypeError',
    'NoTypeError',
    'MultipleTypesError',
    'AbstractModelInstantiationError',
    'InvalidColumnError',
    'UnboundInstanceError',
    'QueryBuiltError',
    'InvalidQueryTypeError',
    'NoSuchColumnError',
]
