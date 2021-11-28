import os
import json
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd
import psycopg2
import sqlalchemy


SECDIST_PATH = '/etc/cloud_validol/secdist.json'


def get_conn_data(conn_data: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    if conn_data is not None:
        return conn_data
    elif os.path.isfile(SECDIST_PATH):
        with open(SECDIST_PATH) as infile:
            data = json.load(infile)

        return data['postgresql']
    else:
        return os.environ


def get_connstr(conn_data: Dict[str, str]) -> str:
    user = conn_data['DATABASE_USER']
    password = conn_data['DATABASE_PASSWORD']
    dbname = conn_data['DATABASE_DB']
    dbhost = conn_data['DATABASE_HOST']

    return f'postgresql+psycopg2://{user}:{password}@{dbhost}/{dbname}'


def get_engine(
    conn_data: Optional[Dict[str, str]] = None
) -> sqlalchemy.engine.base.Engine:
    return sqlalchemy.create_engine(get_connstr(get_conn_data(conn_data)))


def get_connection(
    conn_data: Optional[Dict[str, str]] = None
) -> psycopg2.extensions.connection:
    conn_data = get_conn_data(conn_data)

    return psycopg2.connect(
        user=conn_data['DATABASE_USER'],
        password=conn_data['DATABASE_PASSWORD'],
        dbname=conn_data['DATABASE_DB'],
        host=conn_data['DATABASE_HOST'],
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
