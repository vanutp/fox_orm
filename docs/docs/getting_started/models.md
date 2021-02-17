# Models

You need to declare 2 sets of table definitions: SQLAlchemy's tables and Fox ORM's models

Let's assume that you write your models in the file named `models.py`

## Declare SQLAlchemy tables

From [SQLAlchemy core tutorial](https://docs.sqlalchemy.org/en/14/core/metadata.html):

```python hl_lines="4-5 9-18"
{!../src/getting_started/models.py!}
```

## Declare FoxOrm models

They are very simular to pydantic models, but with some exceptions:

* id field must be declared and must be of type `Optional[int]`
* set `__sqla_table__` to corresponding sqlalchemy's `Table` object

For example:

```python hl_lines="1-2 7 21-29"
{!../src/getting_started/models.py!}
```
