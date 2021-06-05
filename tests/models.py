from typing import Optional, List

from pydantic import BaseModel, Extra
# from sqlalchemy import *
from sqlalchemy import Table, Integer, ForeignKey, Column

from fox_orm import pk, null, FoxOrm
from fox_orm.model import OrmModel
from fox_orm.relations import ManyToMany, OneToMany

# metadata = MetaData()
#
# # a = Table('a', metadata,
# #           Column('id', Integer, primary_key=True),
# #           Column('text', String, nullable=False),
# #           Column('n', Integer, nullable=False),
# #           Column('recursive', JSON, nullable=True),
# #           )
# b = Table('b', metadata,
#           Column('id', Integer, primary_key=True),
#           Column('text2', String, nullable=False),
#           Column('n', Integer, nullable=False),
#           )
# c = Table('c', metadata,
#           Column('id', Integer, primary_key=True),
#           Column('b_id', Integer, ForeignKey('b.id')),
#           Column('d_id', Integer, ForeignKey('b.id')),
#           )
# d = Table('d', metadata,
#           Column('id', Integer, primary_key=True),
#           )
# extra_fields = Table('extra_fields', metadata,
#                      Column('id', Integer, primary_key=True),
#                      )
mid = Table('mid', FoxOrm.metadata,
            Column('a_id', Integer, ForeignKey('a.id'), primary_key=True),
            Column('b_id', Integer, ForeignKey('b.id'), primary_key=True)
            )


class RecursiveTest2(BaseModel):
    a: str


class RecursiveTest(BaseModel):
    a: List[RecursiveTest2]


class A(OrmModel):
    id: Optional[int] = pk, ~null
    text: str
    n: int
    recursive: Optional[RecursiveTest]

    b_objs: ManyToMany['B'] = ManyToMany(to='test_main.B', via=mid, this_id='a_id', other_id='b_id')


print(repr(A.__sqla_table__))


class B(OrmModel):
    id: Optional[int] = pk, ~null
    text2: str
    n: int

    a_objs: ManyToMany[A] = ManyToMany(to='test_main.A', via=mid, this_id='b_id', other_id='a_id')
    c_objs: OneToMany['C'] = OneToMany(to='test_main.C', key='b_id')


print(repr(B.__sqla_table__))


class C(OrmModel):
    id: Optional[int] = pk, ~null
    b_id: Optional[int]
    d_id: Optional[int]


print(repr(C.__sqla_table__))


class D(OrmModel):
    id: Optional[int] = pk, ~null
    c_objs: OneToMany['C'] = OneToMany(to='test_main.C', key='d_id')


print(repr(D.__sqla_table__))


class ExtraFields(OrmModel):
    class Config:
        extra = Extra.allow

    id: Optional[int] = pk, ~null
    _test: str


print(repr(ExtraFields.__sqla_table__))
