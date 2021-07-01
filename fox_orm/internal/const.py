from datetime import datetime, date, time, timedelta

from pydantic import BaseModel
from sqlalchemy import Integer, String, JSON, Boolean, DateTime, Date, Time, Float, Interval

EXCLUDE_KEYS = {'__modified__', '__bound__', '__exclude__'}

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
