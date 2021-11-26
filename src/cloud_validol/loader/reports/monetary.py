import datetime as dt
import io
import logging

import numpy as np
import pandas as pd
import pytz
import requests

logger = logging.getLogger(__name__)


def update(engine, conn):
    logger.info('Start updating stlouisfed data')

    dfs = []
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
        df['sensor'] = sensor
        df = df.rename(columns={graph_id: 'value'})
        del df['DATE']

        dfs.append(df)

    cursor = conn.cursor()
    cursor.execute('TRUNCATE TABLE validol_internal.fredgraph')
    conn.commit()

    pd.concat(dfs).to_sql(
        'fredgraph', engine, schema='validol_internal', index=False, if_exists='append'
    )

    logger.info('Finish updating stlouisfed data')
