import json as json_mod
from typing import Any

from pydantic import BaseModel
from sqlalchemy import JSON, BigInteger
from sqlalchemy.dialects.postgresql import JSONB

from fox_orm.internal.columns import FieldType, ColumnArgument, ColumnFlag


# noinspection PyPep8Naming
# pylint: disable=invalid-name
class int64(int, FieldType):
    sql_type = BigInteger


# noinspection PyPep8Naming
# pylint: disable=invalid-name
class json(FieldType):
    sql_type = JSON(none_as_null=True)


# noinspection PyPep8Naming
# pylint: disable=invalid-name
class jsonb(FieldType):
    sql_type = JSONB(none_as_null=True)


# noinspection PyPep8Naming
# pylint: disable=invalid-name
class default(ColumnArgument):
    key = 'server_default'

    def __init__(self, value: Any):
        self.value = value

    def should_set_server_default(self):
        return isinstance(self.value, (int, str, bool, dict, list, BaseModel)) or self.value is None

    def apply(self, kwargs):
        if self.should_set_server_default():
            if isinstance(self.value, BaseModel):
                kwargs['server_default'] = self.value.json()
            else:
                kwargs['server_default'] = json_mod.dumps(self.value)


null = ColumnFlag('nullable')
pk = ColumnFlag('primary_key')
autoincrement = ColumnFlag('autoincrement')
unique = ColumnFlag('unique')

__all__ = ['int64', 'json', 'jsonb', 'default', 'null', 'pk', 'autoincrement', 'unique']
