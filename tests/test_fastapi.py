import os

import uvicorn as uvicorn
from fastapi import FastAPI
from sqlalchemy import create_engine

from fox_orm import FoxOrm
from tests.models import A, PydanticTest, PydanticTest2

app = FastAPI()


class AInherited(A):
    n: str
    n2: int


@app.get('/', response_model=A)
async def index():
    a_inst = A(text='test_fastapi', n=0)
    await a_inst.save()
    return a_inst


@app.get('/2', response_model=AInherited)
async def index():
    a_inst = AInherited(text='test_fastapi_2', n=1, n2=1, recursive=PydanticTest(a=[PydanticTest2(a='123')]))
    await a_inst.save()
    return a_inst


DB_FILE = 'test.db'
DB_URI = 'sqlite:///test.db'


@app.on_event('startup')
async def connect():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    FoxOrm.metadata.create_all(create_engine(DB_URI))
    FoxOrm.init(DB_URI)
    await FoxOrm.connect()


uvicorn.run(app)
