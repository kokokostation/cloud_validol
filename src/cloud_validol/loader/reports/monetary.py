import datetime as dt
import io
import logging

import numpy as np
import pandas as pd
import psycopg2
import pytz
import requests
import sqlalchemy

logger = logging.getLogger(__name__)


def update(engine: sqlalchemy.engine.base.Engine, conn: psycopg2.extensions.connection):
    logger.info('Start updating stlouisfed data')

    dfs = {}
    for graph_id, sensor in [('BOGMBASEW', 'MBase'), ('ASTDSL', 'TDebt')]:
        response = requests.get(
            url='https://fred.stlouisfed.org/graph/fredgraph.csv',
            params={
                'id': graph_id,
            },
            headers={'Host': 'fred.stlouisfed.org', 'User-Agent': 'Mozilla/5.0'},
        )

        df = pd.read_csv(io.StringIO(response.text))
        df = df.replace('.', np.nan).dropna()
        df['event_dttm'] = df['DATE'].map(
            lambda x: dt.datetime.fromisoformat(x).replace(tzinfo=pytz.UTC)
        )
        df = df.rename(columns={graph_id: sensor.lower()})
        del df['DATE']

        dfs[sensor] = df

    df = pd.merge(dfs['MBase'], dfs['TDebt'], on='event_dttm', how='outer')

    with conn.cursor() as cursor:
        cursor.execute('TRUNCATE TABLE validol_internal.fredgraph_data')
    conn.commit()

    df.to_sql(
        'fredgraph_data',
        engine,
        schema='validol_internal',
        index=False,
        if_exists='append',
        chunksize=10000,
    )

    logger.info('Finish updating stlouisfed data')
