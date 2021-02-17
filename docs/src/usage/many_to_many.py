from typing import Optional

from sqlalchemy import Table, MetaData, Column, Integer, UnicodeText, ForeignKey

from fox_orm.model import OrmModel
from fox_orm.relations import ManyToMany

metadata = MetaData()

users_sqla = Table('users', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('username', UnicodeText, nullable=False, index=True),
                   )

groups_sqla = Table('groups', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('name', UnicodeText, nullable=False, index=True),
                    )

user_group_association = Table('user_group_association', metadata,
                               Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
                               Column('group_id', Integer, ForeignKey('groups.id'), index=True),
                               )


class User(OrmModel):
    __sqla_table__ = users_sqla

    id: Optional[int]
    username: str

    groups: ManyToMany['Group'] = ManyToMany(to='models.Group', via=user_group_association,
                                             this_id='user_id', other_id='group_id')


class Group(OrmModel):
    __sqla_table__ = groups_sqla

    id: Optional[int]
    name: str

    users: ManyToMany['User'] = ManyToMany(to=User, via=user_group_association,
                                           this_id='group_id', other_id='user_id')
