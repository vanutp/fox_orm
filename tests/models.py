from typing import Optional

from sqlalchemy import *

from fox_orm.model import OrmModel
from fox_orm.relations import ManyToMany, OneToMany

metadata = MetaData()

a = Table('a', metadata,
          Column('id', Integer, primary_key=True),
          Column('text', String, nullable=False),
          Column('n', Integer, nullable=False),
          )
b = Table('b', metadata,
          Column('id', Integer, primary_key=True),
          Column('text2', String, nullable=False),
          Column('n', Integer, nullable=False),
          )
c = Table('c', metadata,
          Column('id', Integer, primary_key=True),
          Column('b_id', Integer, ForeignKey('b.id')),
          )
mid = Table('mid', metadata,
            Column('a_id', Integer, ForeignKey('a.id'), primary_key=True),
            Column('b_id', Integer, ForeignKey('b.id'), primary_key=True)
            )


class A(OrmModel):
    __sqla_table__ = a

    id: Optional[int]
    text: str
    n: int

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
