from typing import Optional, List

from pydantic import BaseModel, Extra

from fox_orm import FoxOrm, OrmModel
from fox_orm.fields import pk
from fox_orm.relations import ManyToMany, OneToMany


class RecursiveTest2(BaseModel):
    a: str


class RecursiveTest(BaseModel):
    a: List[RecursiveTest2]


class A(OrmModel):
    pkey: Optional[int] = pk
    text: str
    n: int
    recursive: Optional[RecursiveTest]

    b_objs: ManyToMany['B'] = ManyToMany(to='tests.models.B', via='mid')


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


class ExtraFields(OrmModel):
    class Config:
        extra = Extra.allow

    pkey: Optional[int] = pk
    _test: str


FoxOrm.init_relations()
