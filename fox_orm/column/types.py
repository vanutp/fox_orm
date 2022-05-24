from datetime import timedelta, date, datetime, time

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

int64 = sa.BigInteger
json = sa.JSON(none_as_null=True)
jsonb = JSONB(none_as_null=True)

PY_SQL_TYPES_MAPPING = {
    int: sa.Integer,
    float: sa.Float,
    str: sa.String,
    bool: sa.Boolean,
    datetime: sa.DateTime,
    date: sa.Date,
    time: sa.Time,
    timedelta: sa.Interval,
    dict: json,
    list: json,
}

try:
    from pydantic import BaseModel

    PY_SQL_TYPES_MAPPING[BaseModel] = json
except ImportError:
    pass

__all__ = [
    'PY_SQL_TYPES_MAPPING',
    'int64',
    'json',
    'jsonb',
]
