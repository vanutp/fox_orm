# Row types

You can specify row type

- using type annotations
: Example:

```python
{!../src/usage/02_1_annotations.py!}
```

- using flags
: This is useful if for some reason you want to specify different types
  for Pydantic model and ORM. In this example it's used to use JSONB
  with Python dict type, which is by default mapped to JSON SQL type.

```python
{!../src/usage/02_2_flags.py!}
```

Types specified using flags have higher priority and override types specified
in annotations.

Currently supported types are:

| Python type                    | SQL type         |
| ------------------------------ | ---------------- |
| int                            | int              |
| fox_orm.fields.int64           | bigint           |
| float                          | double precision |
| str                            | text             |
| bool                           | boolean          |
| datetime.datetime              | timestamp        |
| datetime.date                  | date             |
| datetime.time                  | time             |
| datetime.timedelta             | interval         |
| dict                           | json             |
| list                           | json             |
| subclass of pydantic.BaseModel | json             |
| fox_orm.fields.json            | json             |
| fox_orm.fields.jsonb           | jsonb            |
