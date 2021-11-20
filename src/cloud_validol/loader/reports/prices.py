import datetime as dt
import logging
from typing import Dict

import investpy
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

GLOBAL_FROM = '01/01/2010'


def _get_intervals(engine) -> Dict[int, Dict[str, str]]:
    df = pd.read_sql('''
        SELECT 
            info.id AS info_id,
            info.currency_cross AS info_currency_cross,
            MAX(event_dttm) AS last_event_dttm
        FROM validol.investing_prices_info AS info
        LEFT JOIN validol.investing_prices_data AS data 
            ON data.investing_prices_info_id = info.id
        GROUP BY info.id
    ''', engine)

    to_date = dt.date.today()
    to_date_str = to_date.strftime('%d/%m/%Y')

    result = {}
    for _, row in df.iterrows():
        if pd.isnull(row.last_event_dttm):
            logger.info('No data for %s, downloading from %s', row.info_currency_cross, GLOBAL_FROM)

            result[row.info_id] = {
                'currency_cross': row.info_currency_cross,
                'from_date': GLOBAL_FROM,
                'to_date': to_date_str,
            }

            continue

        last_event_dt = row.last_event_dttm.date()
        from_date = last_event_dt + dt.timedelta(days=1)
        from_date_str = from_date.strftime('%d/%m/%Y')

        if from_date == to_date:
            logger.info('Data for %s is already up-to-date', row.info_currency_cross)

            continue

        logger.info('%s is subject to update, downloading from %s', row.info_currency_cross, from_date_str)

        result[row.info_id] = {
            'currency_cross': row.info_currency_cross,
            'from_date': from_date_str,
            'to_date': to_date_str
        }

    return result


def update(engine):
    logger.info('Start updating prices')

    intervals = _get_intervals(engine)

    for info_id, interval in intervals.items():
        df = investpy.get_currency_cross_historical_data(**interval)
        df.index = df.index.map(lambda x: x.replace(tzinfo=pytz.UTC))
        del df['Currency']
        df = df.rename(columns={
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
        })
        df['investing_prices_info_id'] = info_id
        df.to_sql(
            'investing_prices_data',
            engine,
            schema='validol',
            index=True,
            index_label='event_dttm',
            if_exists='append'
        )

    logger.info('Finish updating prices')
