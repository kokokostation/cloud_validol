import datetime as dt
import logging
from typing import Dict

import investpy
import pandas as pd
import psycopg2
import pytz
import sqlalchemy
import tqdm

from cloud_validol.loader.lib import interval_utils

logger = logging.getLogger(__name__)

GLOBAL_FROM = dt.date(2010, 1, 1)


def _dt_serializer(date: dt.date) -> str:
    return date.strftime('%d/%m/%Y')


def _get_intervals(engine: sqlalchemy.engine.base.Engine) -> Dict[int, Dict[str, str]]:
    df = pd.read_sql(
        '''
        SELECT 
            index.series_id,
            MIN(index.currency_cross) AS currency_cross,
            MAX(DATE(event_dttm)) AS last_event_dt
        FROM validol_interface.investing_prices_index AS index
        LEFT JOIN validol_internal.investing_prices_data AS data 
            ON data.series_id = index.series_id
        GROUP BY index.series_id
    ''',
        engine,
    )

    result = {}
    for _, row in df.iterrows():
        interval = interval_utils.get_interval(
            row.currency_cross, row.last_event_dt, GLOBAL_FROM
        )
        if interval is not None:
            from_date, to_date = interval
            result[row.series_id] = {
                'currency_cross': row.currency_cross,
                'from_date': _dt_serializer(from_date),
                'to_date': _dt_serializer(to_date),
            }

    return result


def update(engine: sqlalchemy.engine.base.Engine, conn: psycopg2.extensions.connection):
    logger.info('Start updating prices')

    intervals = _get_intervals(engine)

    for series_id, interval in tqdm.tqdm(intervals.items()):
        df = investpy.get_currency_cross_historical_data(**interval)
        df.index = df.index.map(lambda x: x.replace(tzinfo=pytz.UTC))
        del df['Currency']
        df = df.rename(
            columns={
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
            }
        )
        df['series_id'] = series_id
        df.to_sql(
            'investing_prices_data',
            engine,
            schema='validol_internal',
            index=True,
            index_label='event_dttm',
            if_exists='append',
            chunksize=10000,
        )

    logger.info('Finish updating prices')
