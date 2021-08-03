# Models

Let's assume that you write your models in the file named `models.py`

Fox ORM models are declared the same as pydantic model, with some exceptions:

* you can specify flags (TODO: link) separated by comma after =
  (to set default, use default (TODO: link) flag)
* model must have primary key field. It must be marked as Optional and have pk flag

For example:

```python
{!../src/getting_started/02_models.py!}
```
