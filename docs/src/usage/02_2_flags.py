...
from pydantic import BaseModel
from fox_orm.fields import jsonb


class UserData(BaseModel):
    ...


class User(OrmModel):
    ...
    data: dict = jsonb
