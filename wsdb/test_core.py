import pytest
import pandas as pd

@pytest.fixture
def db():
    from wsdb import wsdb
    return wsdb


def test_query_with_upload(db):
    t = pd.DataFrame(dict(a=[1,2,3], b=[4,5,6]))
    random_table_name = 'abcde1234'
    r = db.query(
        'select * from {}.{}'.format(db.user, random_table_name),
        upload=(random_table_name, t))
    assert (r['a'].values == t['a'].values).all()
    assert (r['b'].values == t['b'].values).all()
    assert random_table_name not in \
        db.tables.query('table_schema == "{}"'.format(db.user)).table_name


