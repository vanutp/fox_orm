# Many to many definition

## Models

You need to declare two models, between which you want a relation:

```python hl_lines="10-18 26-30 36-40"
{!../src/usage/many_to_many.py!}
```

## Association table
Then you need to declare an association table:
```python hl_lines="20-23"
{!../src/usage/many_to_many.py!}
```

## Relation field
Finally, you need to define relation field on each Fox ORM's model:
```python hl_lines="32-33 42-43"
{!../src/usage/many_to_many.py!}
```

* `to` parameter can be either string with full path to model or model class
* `via` parameter should be relation table
* `this_id` is column with id of objects in this table
* `other_id` is column with id of objects in other table
