import os
import logging
import warnings
from functools import lru_cache
from contextlib import contextmanager

import pandas as pd
import records

__all__ = ['wsdb']

curdir = os.path.dirname(os.path.abspath(__file__))

warnings.filterwarnings('ignore', category=UserWarning)

logger = logging.getLogger(__name__)


@contextmanager
def upload(db, df, name, **kwargs):
    """
    Upload and clean up table to database.

    db : records.Database
        database to upload DataFrame
    df : pd.DataFrame
        table to upload
    name : str
        name of the table

    All kwargs passed to pd.DataFrame.to_sql.
    """
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    df.to_sql(name, db.db, **kwargs)
    yield db
    logger.debug('Deleting table')
    db.db.execute('DROP TABLE {name}'.format(name=name))


class WSDB(records.Database):
    """ Whole Sky Database client """

    def __init__(self):
        with open(os.path.join(curdir, 'credentials.txt'), 'r') as f:
            user, pw = f.readlines()
            user, pw = user.strip(), pw.strip()
            super().__init__(
                "postgres://{user}:{pw}@cappc127.ast.cam.ac.uk/wsdb".format(user=user, pw=pw))

        self.user = user
        self._tables = None
        self._columns = None

    def query(self, *args, **kwargs):
        """
        Query database.

        upload : tuple of (str, pd.DataFrame)
            If given, the table is uploaded with name of given string.
            Once the query is executed, the table will be deleted.
        """
        if 'upload' in kwargs:
            logging.debug('upload found in kwargs')
            name, df = kwargs.pop('upload')
            with upload(self, df, name) as db:
                logging.debug('Querying after upload')
                return db.query(*args, **kwargs)
        else:
            q = super().query(*args, **kwargs)
            return q.dataset.df

    @property
    def tables(self):
        """List of all tables available.

        This is cached per instance.
        """
        if self._tables is None:
            query_get_all_tables = """
                select table_schema, table_name, table_type
                from information_schema.tables
                order by table_schema;
                """
            self._tables = self.query(query_get_all_tables)
        return self._tables

    @property
    def columns(self):
        """List of all columns available in all tables.

        This is cached per instance
        """
        if self._columns is None:
            query_get_all_columns = """
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                """
            self._columns = self.query(query_get_all_columns)
        return self._columns

    @lru_cache(maxsize=16)
    def get_table_columns(self, full_table_name):
        """
        Get all columns available for the table

        full_table_name : str
            should be 'schema.tablename' like 'gaia_dr2.gaia_source'
        """
        table_schema, table_name = full_table_name.split('.')
        query_get_table_columns = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='{table_schema}' AND table_name='{table_name}'
            """.format(table_schema=table_schema, table_name=table_name)
        logger.debug('Query:\n'+query_get_table_columns)
        return self.query(query_get_table_columns)

    @property
    def mytables(self):
        """
        List of user table names
        """
        q = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='{user}';
            """.format(user=self.user)
        return self.query(q)

    def upload_df(self, df, name, **kwargs):
        """Upload DataFrame to database

        df : pd.DataFrame
            table to upload
        name : str
            name of the table in the database

        Other kwargs are passed to pd.DataFrame.to_sql
        """
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        df.to_sql(name, self.db, **kwargs)

    def make_q3c_index(self, tablename, ra='ra', dec='dec'):
        """Make Q3C index for the table

        tablename : str
            table to index
        ra, dec : str
            column names of RA and Dec
        """
        self.db.execute("CREATE INDEX ON {tablename} (qc3_ang2ipix({ra}, {dec}));".format(
            tablename=tablename, ra=ra, dec=dec))
        self.db.execute("CLUSTER {tablename}_q3c_ang2ipix_idx ON {tablename};".format(
            tablename=tablename))
        self.db.execute("ANALYZE {tablename};".format(tablename=tablename))


wsdb = WSDB()