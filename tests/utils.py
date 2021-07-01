from sqlalchemy import Table


def schema_to_set(table: Table):
    return {(x.name, x.type.__class__) for x in table.columns.values()}
