from datetime import datetime
from typing import Optional

from fox_orm.fields import pk, default, null
from fox_orm.model import OrmModel


class User(OrmModel):
    id: Optional[int] = pk
    first_name: str
    last_name: Optional[str]
    username: str
    birthday: Optional[datetime]
    data: dict = default({}), ~null
