# Many to many usage

## Fetch

```python
user = await User.get(1)
await user.groups.fetch()
```

Or:

```python
user = await User.get(1)
await user.fetch_related('groups')
```

## Contains

```python
group = await Group.get(1)
user = await User.get(1)
await user.groups.fetch()
contains = group in user.groups
```

## Add

You do not need to call `instance.relation.fetch()` if you need to just add to/delete from relation

```python
user.groups.add(group)
await user.groups.save()

user.groups.delete(group)
await user.groups.save()
```

Or:

```python
await user.groups.add(group)

await user.groups.delete(group)
```
