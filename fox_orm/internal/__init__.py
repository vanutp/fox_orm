from abc import ABC, abstractmethod
from datetime import datetime, date, time, timedelta
from typing import Type, Any

from pydantic import BaseModel
from sqlalchemy import Integer, String, JSON, Boolean, DateTime, Date, Time, Float, Interval

from fox_orm.internal.utils import NonInstantiable

json_fix_none = JSON(none_as_null=True)

PY_SQL_TYPES_MAPPING = {
    int: Integer,
    float: Float,
    str: String,
    bool: Boolean,

    datetime: DateTime,
    date: Date,
    time: Time,
    timedelta: Interval,

    dict: json_fix_none,
    list: json_fix_none,
    BaseModel: json_fix_none,
}


class FieldType(NonInstantiable):
    sql_type: Type


class ColumnArgument(ABC):
    key: str

    def apply(self, kwargs):
        kwargs[self.key] = self.get_value()

    @abstractmethod
    def get_value(self) -> Any:
        ...


class ColumnFlag(ColumnArgument):
    inverse: bool

    def get_value(self):
        return not self.inverse

    def __init__(self, key: str, inverse: bool = False):
        self.key = key
        self.inverse = inverse

    def __invert__(self):
        return ColumnFlag(self.key, not self.inverse)
