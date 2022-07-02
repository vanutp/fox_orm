import datetime
from typing import Optional, List

from pydantic import BaseModel, Extra

from fox_orm import FoxOrm, OrmModel
from fox_orm.column.flags import pk
from fox_orm.relations import ManyToMany, OneToMany


class PydanticTest2(BaseModel):
    a: str


class PydanticTest(BaseModel):
    a: List[PydanticTest2]
    b: int


class A(OrmModel):
    pkey: Optional[int] = pk
    text: str
    n: int
    pydantic: Optional[PydanticTest]

    b_objs: ManyToMany['B'] = ManyToMany(to='tests.models.B', via='mid')

    async def extra_function(self):
        pass


class B(OrmModel):
    pkey: Optional[int] = pk
    text2: str
    n: int

    a_objs: ManyToMany[A] = ManyToMany(to='tests.models.A', via='mid')
    c_objs: OneToMany['C'] = OneToMany(to='tests.models.C', key='b_id')


class C(OrmModel):
    pkey: Optional[int] = pk
    b_id: Optional[int]
    d_id: Optional[int]


class D(OrmModel):
    pkey: Optional[int] = pk
    c_objs: OneToMany['C'] = OneToMany(to='tests.models.C', key='d_id')


class E(OrmModel):
    pkey: Optional[int] = pk
    dt: datetime.datetime


FoxOrm.init_relations()
