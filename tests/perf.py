import asyncio
import os
from time import time

from sqlalchemy import create_engine

from fox_orm import FoxOrm
from tests.models import metadata, a, A

DB_FILE = 'test.db'
DB_URI = 'sqlite:///test.db'

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
FoxOrm.init(DB_URI)
metadata.create_all(create_engine(DB_URI))

ITERATIONS = 1000


async def main():
    print('Simple insert')
    time_start = time()
    for i in range(ITERATIONS):
        await FoxOrm.db.execute(a.insert(), {
            'text': str(i),
            'n': i,
        })
    print('- Databases', (time() - time_start) / ITERATIONS)

    time_start = time()
    for i in range(ITERATIONS):
        a_obj = A(text=str(i), n=i)
        await a_obj.save()
    print('- FoxOrm', (time() - time_start) / ITERATIONS)


asyncio.run(main())
