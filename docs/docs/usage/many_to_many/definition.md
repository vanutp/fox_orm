# Many to many definition

## Models

You need to declare two models, between which you want a relation:

```python hl_lines="8-10 15-17"
{!../src/usage/many_to_many.py!}
```

## Relation field
Then you need to define relation field on each Fox ORM model:
```python hl_lines="12 19"
{!../src/usage/many_to_many.py!}
```

* `to` parameter can be either string with full path to model or model class
* `via` parameter should be name of the relation table

## init_relations

If you use relations in file, you should place `FoxOrm.init_relations()` at the end of
your file

```python hl_lines="22"
{!../src/usage/many_to_many.py!}
```
