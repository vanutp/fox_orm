import typing

from sqlalchemy import Column

from fox_orm.column import ColumnArgument
from fox_orm.column.types import PY_SQL_TYPES_MAPPING
from fox_orm.exceptions import OrmException
from fox_orm.internal.utils import (
    lenient_issubclass,
    parse_type,
    is_sqla_type,
    UNSUPPORTED_TYPE,
)


def construct_column(name, annotation, args) -> Column:
    if not isinstance(args, tuple):
        args = (args,)

    final_type = None
    parsed_type, required = parse_type(annotation)
    parsed_type = typing.get_origin(parsed_type) or parsed_type
    if is_sqla_type(parsed_type):
        final_type = parsed_type
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
    for arg in args:
        if is_sqla_type(arg) or arg in PY_SQL_TYPES_MAPPING:
            if type_specified_via_arg:
                raise OrmException('More than one type specified in arguments')
            type_specified_via_arg = True
            if is_sqla_type(arg):
                final_type = arg
            elif arg in PY_SQL_TYPES_MAPPING:
                final_type = PY_SQL_TYPES_MAPPING[arg]

        elif isinstance(arg, ColumnArgument):
            arg.apply(column_args, column_kwargs)
        else:
            raise OrmException(
                f'Argument {arg} has unknown type {parsed_type(arg).__qualname__}'
            )

    if final_type is None:
        if parsed_type is UNSUPPORTED_TYPE:
            raise OrmException(
                f'Unsupported type {annotation} specified for column {name}'
            )
        raise OrmException(f'Bad type specified for column {name}')

    return Column(name, final_type, *column_args, **column_kwargs)
