from datetime import datetime
from typing import Optional

from sqlalchemy import Table, MetaData, Column, Integer, UnicodeText, DateTime
from sqlalchemy.dialects.postgresql import JSONB

from fox_orm.model import OrmModel

metadata = MetaData()

users_sqla = Table('users', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('first_name', UnicodeText, nullable=False),
                   Column('last_name', UnicodeText, nullable=True),
                   Column('username', UnicodeText, nullable=False, index=True),
                   Column('birthday', DateTime, nullable=True),
                   Column('data', JSONB, nullable=False, server_default='{}')
                   )


class User(OrmModel):
    __sqla_table__ = users_sqla

    id: Optional[int]
    first_name: str
    last_name: Optional[str]
    username: str
    birthday: Optional[datetime]
    data: dict = {}
