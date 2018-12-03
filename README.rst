wsdb
====

Routines for accessing wsdb. Not of general interest.

Usage
-----

Put two-line text in wsdb/credentials.txt as in
  user
  password
and
.. code-block:: python
  from wsdb import wsdb
  rows = wsdb.query("<query>")
  rows.dataset.df

Installation
------------

Requirements
^^^^^^^^^^^^
- `records <https://github.com/kennethreitz/records>`_
- `psycopg2 <http://initd.org/psycopg/>`_



Authors
-------

`wsdb` was written by `Semyeong Oh <smohspace@outlook.com>`_.
