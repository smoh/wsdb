wsdb
====

Routines for accessing wsdb, a postgres database, from python.
Not of general interest.

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
  df = wsdb.query("<query>")
```

Requirements
------------

- [records](https://github.com/kennet/records)
- [psycopg2](http://initd.org/psycopg/)
