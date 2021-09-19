from types import FunctionType
from typing import Type, Tuple

from pydantic import BaseConfig
from pydantic.fields import ModelField, FieldInfo, Field
from pydantic.utils import lenient_issubclass
from sqlalchemy import Column

from fox_orm.fields import default as arg_default
from fox_orm.exceptions import OrmException
from fox_orm.internal.columns import FieldType, ColumnArgument
from fox_orm.internal.const import PY_SQL_TYPES_MAPPING


def parse_type(type_: Type):
    class Config(BaseConfig):
        arbitrary_types_allowed = True

    parsed = ModelField(name='', type_=type_, model_config=Config, class_validators=None)
    return parsed.type_, parsed.required


MISSING = object()


# pylint: disable=too-many-branches
def construct_column(name, annotation, args) -> Tuple[Column, FieldInfo]:
    if not isinstance(args, tuple):
        args = (args,)

    final_type = None
    parsed_type, required = parse_type(annotation)
    if lenient_issubclass(parsed_type, FieldType):
        # false positive
        # pylint: disable=no-member
        final_type = parsed_type.sql_type
    elif parsed_type in PY_SQL_TYPES_MAPPING:
        final_type = PY_SQL_TYPES_MAPPING[parsed_type]
    else:
        for k, v in PY_SQL_TYPES_MAPPING.items():
            if issubclass(parsed_type, k):
                final_type = v
                break
    column_args = []
    column_kwargs = {}
    if required:
        column_kwargs['nullable'] = False

    value = Field()

    type_specified_via_arg = False
    for arg in args:
        if lenient_issubclass(arg, FieldType) or arg in PY_SQL_TYPES_MAPPING:
            if type_specified_via_arg:
                raise OrmException('More than one type specified in arguments')
            type_specified_via_arg = True
        if lenient_issubclass(arg, FieldType):
            final_type = arg.sql_type
        elif arg in PY_SQL_TYPES_MAPPING:
            final_type = PY_SQL_TYPES_MAPPING[arg]

        elif isinstance(arg, ColumnArgument):
            if isinstance(arg, arg_default):
                if isinstance(arg.value, FunctionType):
                    value = Field(default_factory=arg.value)
                else:
                    value = Field(default=arg.value)

            arg.apply(column_args, column_kwargs)
        else:
            raise OrmException(f'Argument {arg} has unknown type {type(arg).__qualname__}')

    if final_type is None:
        raise OrmException('No type specified')

    return Column(name, final_type, *column_args, **column_kwargs), value
