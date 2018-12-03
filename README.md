wsdb
====

Routines for accessing wsdb. Not of general interest.

Usage
-----

Put two-line text in `wsdb/credentials.txt` as in
```
  user
  password
```

and

```python
  from wsdb import wsdb
  rows = wsdb.query("<query>")
  rows.dataset.df
```

Requirements
------------

- [records](https://github.com/kennet/records)
- [psycopg2](http://initd.org/psycopg/)
