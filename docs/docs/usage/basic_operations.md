# Basic operations

## Create and insert

To create a user just create an instance of model (without filling id field), as you would in pydantic

Then execute `await instance.save()`. If the row is already in the table, it will be updated. Otherwise, it will be
inserted

```python
user = User(first_name='Test', username='test')
await user.save()
```

## Select and update

To select rows from a table, you can use `Model.select`

First argument of `Model.select` should be SQLAlchemy's expression

You can also optionally specify `order_by` (in SQLAlchemy format, for example `order_by=User.c.birthday`)

Afterwards, you can change instance's fields and execute `await instance.save()` to save changes

```python
user = await User.select(User.c.username == 'test')
user.last_name = 'Last name'
await user.save()
```

### Mutable fields

Due to python limitations, you need to call `instance.flag_modified('field_name')`
after modifying fields, which values are mutable objects in Python (for example JSON/JSONB)

```python
user.data['additional_field'] = 'value'
user.flag_modified('data')
await user.save()
```

## Get

`Model.get(obj_id)` is a shorthand for `Model.select(Model.c.id == obj_id)`

For example:

```python
user = await User.get(1)
```

This is not necessary if you set the whole field, like this:

```python
user.data = {'additional_field': 'value'}
await user.save()
```

## Select all

You can select multiple rows using `Model.select_all`

You can optionally specify `order_by` (similarly to `Model.select`), `limit` and `offset`

```python
from datetime import datetime, timedelta

users = await User.select_all(User.c.birthday > datetime.now() - timedelta(days=365 * 18),
                              order_by=User.c.birthday.desc())
for user in users:
    print(user.first_name, user.last_name, user.birthday)
```

## Select exists

You can check if row exists using `Model.exists`

```python
exists = await User.exists(User.c.username == 'test')
```

## Delete

You can delete object by calling `Model.delete(expression)` or `instance.delete()`

```python
user = await User.select(User.c.username == 'test')
await user.delete()
```

Or:

```python
await User.delete(User.c.username == 'test')
```