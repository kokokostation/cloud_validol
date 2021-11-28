import datetime as dt
import io
import logging
import time
from typing import Optional
from typing import Tuple

import pandas as pd
import psycopg2
import pytz
import requests
import sqlalchemy
import tqdm

from cloud_validol.loader.lib import interval_utils
from cloud_validol.loader.lib import pg

logger = logging.getLogger(__name__)

GLOBAL_FROM = dt.date(2012, 11, 1)
CONTRACT_TYPE_MAPPING = {'F': 'Futures', 'C': 'Option call', 'P': 'Option put'}
CSV_MAPPING = {
    'clients_in_long': 'lq',
    'clients_in_short': 'sq',
    'short_position': 's',
    'long_position': 'l',
}
PHYS_MAPPING = {0: 'u', 1: 'f'}


def _dt_serializer(date: dt.date) -> str:
    return date.strftime('%Y%m%d')


def _download_date(date: dt.date) -> Optional[pd.DataFrame]:
    response = requests.get(
        'https://www.moex.com/ru/derivatives/open-positions-csv.aspx',
        params={'d': _dt_serializer(date)},
        headers={'User-Agent': 'Mozilla/5.0'},
    )

    df = pd.read_csv(
        io.StringIO(response.text),
        parse_dates=['moment'],
        date_parser=lambda x: dt.datetime.fromisoformat(x).replace(tzinfo=pytz.UTC),
    )

    if df.empty:
        return None

    df = df.rename(columns={'moment': 'event_dttm', 'isin': 'code'})

    df.name = df.apply(
        lambda row: '{} ({})'.format(
            row['name'], CONTRACT_TYPE_MAPPING[row.contract_type]
        ),
        axis=1,
    )
    df.iz_fiz.fillna(0, inplace=True)

    keys = ['event_dttm', 'name']

    return pd.DataFrame(
        [
            {
                **{
                    '{}{}'.format(PHYS_MAPPING[row.iz_fiz], value): row[key]
                    for _, row in content.iterrows()
                    for key, value in CSV_MAPPING.items()
                },
                **dict(zip(keys, group)),
            }
            for group, content in df.groupby(keys)
        ]
    )


def _get_interval(
    engine: sqlalchemy.engine.base.Engine,
) -> Optional[Tuple[dt.date, dt.date]]:
    df = pd.read_sql(
        '''
        SELECT MAX(DATE(event_dttm)) AS last_event_dt
        FROM validol_internal.moex_derivatives_data
    ''',
        engine,
    )

    last_event_dt = df.iloc[0].last_event_dt

    return interval_utils.get_interval('moex', last_event_dt, GLOBAL_FROM)


def update(engine: sqlalchemy.engine.base.Engine, conn: psycopg2.extensions.connection):
    logger.info('Start updating moex data')

    interval = _get_interval(engine)
    if interval is None:
        return

    from_date, to_date = interval
    dfs = []
    for date in tqdm.tqdm(pd.date_range(from_date, to_date)):
        date_data = _download_date(date)
        if date_data is not None:
            dfs.append(_download_date(date))

        time.sleep(2)

    result_df = pd.concat(dfs)

    unique_derivative_names = list(result_df.name.unique())
    with conn.cursor() as cursor:
        cursor.execute(
            '''
            INSERT INTO validol_internal.moex_derivatives_info (name)
            SELECT UNNEST(%s)
            ON CONFLICT (name) DO UPDATE SET
                name = EXCLUDED.name
            RETURNING id
        ''',
            (unique_derivative_names,),
        )

        derivative_name_map_id = dict(
            zip(unique_derivative_names, pg.extract_ids_from_cursor(cursor))
        )

    conn.commit()

    result_df['moex_derivatives_info_id'] = [
        derivative_name_map_id[name] for name in result_df['name']
    ]
    del result_df['name']

    result_df.to_sql(
        'moex_derivatives_data',
        engine,
        schema='validol_internal',
        index=False,
        if_exists='append',
    )

    logger.info('Finish updating moex data')
