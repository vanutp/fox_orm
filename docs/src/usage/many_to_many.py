from typing import Optional

from fox_orm import FoxOrm, OrmModel
from fox_orm.fields import pk
from fox_orm.relations import ManyToMany


class User(OrmModel):
    id: Optional[int] = pk
    username: str

    groups: ManyToMany['Group'] = ManyToMany(to='models.Group', via='user_group_association')


class Group(OrmModel):
    id: Optional[int] = pk
    name: str

    users: ManyToMany['User'] = ManyToMany(to=User, via='user_group_association')


FoxOrm.init_relations()
