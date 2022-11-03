from datetime import timedelta, date, datetime, time

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from fox_orm.internal.pydantic_compat import BaseModel

int64 = sa.BigInteger()
json = sa.JSON(none_as_null=True)
jsonb = JSONB(none_as_null=True)

PY_SQL_TYPES_MAPPING = {
    int: sa.Integer(),
    float: sa.Float(),
    str: sa.String(),
    bool: sa.Boolean(),
    datetime: sa.DateTime(),
    date: sa.Date(),
    time: sa.Time(),
    timedelta: sa.Interval(),
    dict: json,
    list: json,
    BaseModel: json,
}

__all__ = [
    'PY_SQL_TYPES_MAPPING',
    'int64',
    'json',
    'jsonb',
]
