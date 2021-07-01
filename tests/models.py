from typing import Optional, List

from pydantic import BaseModel, Extra

from fox_orm import pk, FoxOrm
from fox_orm.model import OrmModel
from fox_orm.relations import ManyToMany, OneToMany


class RecursiveTest2(BaseModel):
    a: str


class RecursiveTest(BaseModel):
    a: List[RecursiveTest2]


class A(OrmModel):
    id: Optional[int] = pk
    text: str
    n: int
    recursive: Optional[RecursiveTest]

    b_objs: ManyToMany['B'] = ManyToMany(to='tests.models.B', via='mid')


class B(OrmModel):
    id: Optional[int] = pk
    text2: str
    n: int

    a_objs: ManyToMany[A] = ManyToMany(to='tests.models.A', via='mid')
    c_objs: OneToMany['C'] = OneToMany(to='tests.models.C', key='b_id')


class C(OrmModel):
    id: Optional[int] = pk
    b_id: Optional[int]
    d_id: Optional[int]


class D(OrmModel):
    id: Optional[int] = pk
    c_objs: OneToMany['C'] = OneToMany(to='tests.models.C', key='d_id')


class ExtraFields(OrmModel):
    class Config:
        extra = Extra.allow

    id: Optional[int] = pk
    _test: str

FoxOrm.init_relations()
