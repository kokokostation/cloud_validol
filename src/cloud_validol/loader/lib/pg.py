from typing import Any
from typing import Iterable
from typing import List
from typing import Tuple

import pandas as pd
import psycopg2
import sqlalchemy

from cloud_validol.lib import secdist


def get_engine() -> sqlalchemy.engine.base.Engine:
    conn_data = secdist.get_pg_conn_data()
    connstr = (
        f'postgresql+psycopg2://{conn_data.user}:{conn_data.password}@'
        f'{conn_data.host}/{conn_data.dbname}'
    )

    return sqlalchemy.create_engine(connstr)


def get_connection() -> psycopg2.extensions.connection:
    conn_data = secdist.get_pg_conn_data()

    return psycopg2.connect(
        user=conn_data.user,
        password=conn_data.password,
        dbname=conn_data.dbname,
        host=conn_data.host,
    )


def insert_on_conflict_do_nothing(
    table: pd.io.sql.SQLTable,
    conn: sqlalchemy.engine.base.Connection,
    keys: List[str],
    data_iter: Iterable[Iterable[Any]],
):
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_stmt = sqlalchemy.dialects.postgresql.insert(
        table.table, values=data, bind=conn
    ).on_conflict_do_nothing()
    conn.execute(insert_stmt)


def extract_ids_from_cursor(cursor: Iterable[Tuple[int]]):
    return [id_ for id_, in cursor]
