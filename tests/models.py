from typing import Optional, List

from pydantic import BaseModel, Extra
from sqlalchemy import *

from fox_orm.model import OrmModel
from fox_orm.relations import ManyToMany, OneToMany

metadata = MetaData()

a = Table('a', metadata,
          Column('id', Integer, primary_key=True),
          Column('text', String, nullable=False),
          Column('n', Integer, nullable=False),
          Column('recursive', JSON, nullable=True),
          )
b = Table('b', metadata,
          Column('id', Integer, primary_key=True),
          Column('text2', String, nullable=False),
          Column('n', Integer, nullable=False),
          )
c = Table('c', metadata,
          Column('id', Integer, primary_key=True),
          Column('b_id', Integer, ForeignKey('b.id')),
          Column('d_id', Integer, ForeignKey('b.id')),
          )
d = Table('d', metadata,
          Column('id', Integer, primary_key=True),
          )
extra_fields = Table('extra_fields', metadata,
                     Column('id', Integer, primary_key=True),
                     )
mid = Table('mid', metadata,
            Column('a_id', Integer, ForeignKey('a.id'), primary_key=True),
            Column('b_id', Integer, ForeignKey('b.id'), primary_key=True)
            )


class RecursiveTest2(BaseModel):
    a: str


class RecursiveTest(BaseModel):
    a: List[RecursiveTest2]


class A(OrmModel):
    __sqla_table__ = a

    id: Optional[int]
    text: str
    n: int
    recursive: Optional[RecursiveTest]

    b_objs: ManyToMany['B'] = ManyToMany(to='test_main.B', via=mid, this_id='a_id', other_id='b_id')


class B(OrmModel):
    __sqla_table__ = b

    id: Optional[int]
    text2: str
    n: int

    a_objs: ManyToMany[A] = ManyToMany(to='test_main.A', via=mid, this_id='b_id', other_id='a_id')
    c_objs: OneToMany['C'] = OneToMany(to='test_main.C', key='b_id')


class C(OrmModel):
    __sqla_table__ = c

    id: Optional[int]
    b_id: Optional[int]
    d_id: Optional[int]


class D(OrmModel):
    __sqla_table__ = d

    id: Optional[int]
    c_objs: OneToMany['C'] = OneToMany(to='test_main.C', key='d_id')


class ExtraFields(OrmModel):
    class Config:
        extra = Extra.allow

    __sqla_table__ = extra_fields

    id: Optional[int]
    _test: str
