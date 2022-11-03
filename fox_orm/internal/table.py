import typing
from typing import Any

from sqlalchemy import Column

from fox_orm.column import ColumnArgument
from fox_orm.column.types import PY_SQL_TYPES_MAPPING
from fox_orm.exceptions import (
    OrmError,
    MultipleTypesError,
    UnsupportedTypeError,
    NoTypeError,
)
from fox_orm.internal.utils import (
    lenient_issubclass,
    parse_type,
    to_sqla_type_instance,
    UNSUPPORTED_TYPE,
)


def construct_column(name: str, annotation: Any, args: Any) -> Column:
    if not isinstance(args, tuple):
        args = (args,)

    final_type = None
    parsed_type, required = parse_type(annotation)
    parsed_type = typing.get_origin(parsed_type) or parsed_type
    if (sqla_type := to_sqla_type_instance(parsed_type)) is not None:
        final_type = sqla_type
    elif parsed_type in PY_SQL_TYPES_MAPPING:
        final_type = PY_SQL_TYPES_MAPPING[parsed_type]
    else:
        for k, v in PY_SQL_TYPES_MAPPING.items():
            if lenient_issubclass(parsed_type, k):
                final_type = v
                break
    column_args = []
    column_kwargs = {}
    if required:
        column_kwargs['nullable'] = False

    type_specified_via_arg = False
    type_args = []
    other_args = []
    for arg in args:
        if to_sqla_type_instance(arg) or arg in PY_SQL_TYPES_MAPPING:
            type_args.append(arg)
        elif isinstance(arg, ColumnArgument):
            other_args.append(arg)
        else:
            raise OrmError(f'Argument {arg} has unknown type {type(arg).__qualname__}')

    for arg in type_args:
        if type_specified_via_arg:
            raise MultipleTypesError(name)
        type_specified_via_arg = True
        if sqla_type := to_sqla_type_instance(arg):
            final_type = sqla_type
        elif arg in PY_SQL_TYPES_MAPPING:
            final_type = PY_SQL_TYPES_MAPPING[arg]
    if final_type is None:
        if parsed_type is UNSUPPORTED_TYPE:
            raise UnsupportedTypeError(annotation, name)
        raise NoTypeError(name)

    for arg in other_args:
        arg.apply(column_args, column_kwargs, final_type)

    return Column(name, final_type, *column_args, **column_kwargs)
