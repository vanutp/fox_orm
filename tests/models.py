from datetime import datetime

import sqlalchemy as sa
from fox_orm import FoxOrm, OrmModel
from fox_orm.column import int64
from fox_orm.column.flags import pkey, default, fkey, autoincrement
from fox_orm.relations import ManyToMany, OneToMany, ObjectLink
from pydantic import BaseModel


class User(OrmModel):
    id: int64 = pkey
    username: str
    created_at: datetime = default(sa.func.now())
    balance: int = default(100)
    bio: str = default('a few words about me')

    groups: ManyToMany['Group'] = ManyToMany(
        to='tests.models.Group', via='user_group_assoc'
    )

    async def extra_function(self):
        pass


class Group(OrmModel):
    id: int = pkey
    name: str

    users: ManyToMany[User] = ManyToMany(to=User, via='user_group_assoc')
    admins: ManyToMany[User] = ManyToMany(to=User, key='user_admins_assoc')
    messages: OneToMany['Message'] = OneToMany(to='tests.models.Message')


class Message(OrmModel):
    group_id: int64 = fkey(Group.id), pkey
    group: Group = ObjectLink(to=Group, key='group_id')
    message_id: int64 = pkey, autoincrement

    text: str
    sender_id: int64 = fkey(User.id)
    sender: User = ObjectLink(to=User, key='user_messages_assoc')

    metadata: ObjectLink['MessageMetadata'] = ObjectLink(
        to='tests.models.MessageMetadata', key=('group_id', 'message_id')
    )


class AdditionalMeta(BaseModel):
    some_field: str
    another_field: int


class MessageMetadata(OrmModel):
    group_id: int64 = fkey(Group.id), pkey
    message_id: int64 = pkey

    photo_id: int | None

    message: ObjectLink[Message] = ObjectLink(
        to=Message, key=('group_id', 'message_id')
    )
    __table_args__ = [
        sa.ForeignKeyConstraint(
            ('group_id', 'message_id'),
            (Message.group_id, Message.message_id),
        ),
    ]


FoxOrm.init_relations()
