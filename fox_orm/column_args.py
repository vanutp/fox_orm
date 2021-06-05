from types import FunctionType
from typing import Any

from pydantic.utils import smart_deepcopy
from sqlalchemy import JSON, BigInteger
from sqlalchemy.dialects.postgresql import JSONB

from fox_orm.internal import FieldType, NonInstantiable, ColumnArgument, ColumnFlag


# noinspection PyPep8Naming
class int64(int, FieldType):
    sql_type = BigInteger


# noinspection PyPep8Naming
class json(FieldType):
    sql_type = JSON(none_as_null=True)


# noinspection PyPep8Naming
class jsonb(FieldType):
    sql_type = JSONB(none_as_null=True)


# noinspection PyPep8Naming
class default(ColumnArgument):
    def __init__(self, value: Any):
        self.value = value

    def should_set_server_default(self):
        return not isinstance(self.value, FunctionType)

    def apply(self, kwargs):
        if self.should_set_server_default():
            kwargs['server_default'] = smart_deepcopy(self.value)


null = ColumnFlag('nullable')
pk = ColumnFlag('primary_key')
autoincrement = ColumnFlag('autoincrement')
unique = ColumnFlag('unique')


__all__ = ['int64', 'json', 'jsonb', 'default', 'null', 'pk', 'autoincrement', 'unique']
