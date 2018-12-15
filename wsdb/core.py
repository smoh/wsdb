import os
import logging
import warnings
import records
from functools import lru_cache

__all__ = ['wsdb']

curdir = os.path.dirname(os.path.abspath(__file__))

warnings.filterwarnings('ignore', category=UserWarning)

logger = logging.getLogger(__name__)

class WSDB(records.Database):
    """ Whole Sky Database client """

    def __init__(self):
        with open(os.path.join(curdir, 'credentials.txt'), 'r') as f:
            user, pw = f.readlines()
            user, pw = user.strip(), pw.strip()
            super().__init__(
                "postgres://{user}:{pw}@cappc127.ast.cam.ac.uk/wsdb".format(user=user, pw=pw))

        self._tables = None
        self._columns = None

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
            self._tables = self.query(query_get_all_tables).dataset.df
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
            self._columns = self.query(query_get_all_columns).dataset.df
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


wsdb = WSDB()