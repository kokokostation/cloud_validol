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
            info.id AS info_id,
            info.currency_cross AS info_currency_cross,
            MAX(DATE(event_dttm)) AS last_event_dt
        FROM validol_internal.investing_prices_info AS info
        LEFT JOIN validol_internal.investing_prices_data AS data 
            ON data.investing_prices_info_id = info.id
        GROUP BY info.id
    ''',
        engine,
    )

    result = {}
    for _, row in df.iterrows():
        interval = interval_utils.get_interval(
            row.info_currency_cross, row.last_event_dt, GLOBAL_FROM
        )
        if interval is not None:
            from_date, to_date = interval
            result[row.info_id] = {
                'currency_cross': row.info_currency_cross,
                'from_date': _dt_serializer(from_date),
                'to_date': _dt_serializer(to_date),
            }

    return result


def update(engine: sqlalchemy.engine.base.Engine, conn: psycopg2.extensions.connection):
    logger.info('Start updating prices')

    intervals = _get_intervals(engine)

    for info_id, interval in tqdm.tqdm(intervals.items()):
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
        df['investing_prices_info_id'] = info_id
        df.to_sql(
            'investing_prices_data',
            engine,
            schema='validol_internal',
            index=True,
            index_label='event_dttm',
            if_exists='append',
        )

    logger.info('Finish updating prices')
