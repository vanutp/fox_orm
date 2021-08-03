# Generating tables

After you declared your models, you should generate them.
You can either do it using create_all or use Alembic to manage migrations.

## Using create_all

```python
{!../src/getting_started/03_generate_using_create_all.py!}
```

## Using Alembic

All generated tables use `FoxOrm.metadata` as metadata.
If you use Alembic, you should point it to this variable after
importing file with models in env.py.

See [Databases documentation](https://www.encode.io/databases/tests_and_migrations/)