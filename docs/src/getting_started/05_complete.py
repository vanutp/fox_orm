import asyncio
from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine

from fox_orm import FoxOrm
from fox_orm.fields import pk, default, null
from fox_orm.model import OrmModel


class User(OrmModel):
    id: Optional[int] = pk
    first_name: str
    last_name: Optional[str]
    username: str
    birthday: Optional[datetime]
    data: dict = default({}), ~null


DB_URI = 'sqlite:///example.db'

engine = create_engine(DB_URI)
FoxOrm.metadata.create_all(engine)

FoxOrm.init(DB_URI)


async def main():
    await FoxOrm.connect()
    user = User(first_name='vanutp', last_name='fox', username='vanutp')
    await user.save()
    uid = user.id
    print(uid)

    found_user = await User.get(uid)
    print(found_user)

    await FoxOrm.disconnect()


asyncio.run(main())
