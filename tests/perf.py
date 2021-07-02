import asyncio
import os
from time import time

from sqlalchemy import create_engine

from fox_orm import FoxOrm
from tests.models import A

DB_FILE = 'test.db'
DB_URI = 'sqlite:///test.db'

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
FoxOrm.init(DB_URI)
FoxOrm.metadata.create_all(create_engine(DB_URI))

ITERATIONS = 300


async def main():
    print('Simple insert')
    time_start = time()
    for i in range(ITERATIONS):
        await FoxOrm.db.execute(A.__table__.insert(), {
            'text': 'test',
            'n': i,
        })
    print('- Databases', (time() - time_start) / ITERATIONS)

    time_start = time()
    for i in range(ITERATIONS):
        a_obj = A(text='test2', n=i)
        await a_obj.save()
    print('- FoxOrm', (time() - time_start) / ITERATIONS)

    print('Select all')

    time_start = time()
    for i in range(ITERATIONS):
        data = await FoxOrm.db.fetch_all(A.__table__.select().where(A.__table__.c.text == 'test'))
    print('- Databases', (time() - time_start) / ITERATIONS)

    time_start = time()
    for i in range(ITERATIONS):
        data = await A.select_all(A.c.text == 'test2', skip_parsing=True)
    print('- FoxOrm', (time() - time_start) / ITERATIONS)


asyncio.run(main())
