import dataclasses
import datetime as dt
import io
import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd
import psycopg2
import pytz
import sqlalchemy

from cloud_validol.loader.lib import pg

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DerivativeConfig:
    name: str
    source: str
    table_name: str
    platform_code_col: str
    derivative_name_col: str
    date_col: str
    date_format: str
    data_cols: Dict[str, str]
    report_type: str
    initial_from_year: int
    download_config: Any = None


@dataclasses.dataclass
class UpdateInterval:
    load_initial: bool
    years_to_load: List[int]


def get_interval(
    engine: sqlalchemy.engine.base.Engine, config: DerivativeConfig
) -> Optional[UpdateInterval]:
    df = pd.read_sql(
        f'''
            SELECT 
                MAX(DATE(data.event_dttm)) AS last_event_dt
            FROM validol_internal.{config.table_name} AS data
            INNER JOIN validol_internal.cot_derivatives_info AS info
                ON data.cot_derivatives_info_id = info.id
            INNER JOIN validol_internal.cot_derivatives_platform AS platform
                ON info.cot_derivatives_platform_id = platform.id
            WHERE data.report_type = %(report_type)s AND platform.source = %(platform_source)s
        ''',
        engine,
        params={'report_type': config.report_type, 'platform_source': config.source},
    )

    last_event_dt = df.iloc[0].last_event_dt
    today = dt.date.today()

    if last_event_dt is None:
        logger.info('No data for %s, downloading from scratch', config.name)

        return UpdateInterval(
            load_initial=True,
            years_to_load=list(range(config.initial_from_year, today.year + 1)),
        )

    if last_event_dt >= today:
        logger.info('Data for %s is already up-to-date', config.name)

        return None

    logger.info(
        '%s is subject to update, downloading from %s', config.name, last_event_dt.year
    )

    return UpdateInterval(
        load_initial=False,
        years_to_load=list(range(last_event_dt.year, today.year + 1)),
    )


def process_raw_dataframe(
    config: DerivativeConfig,
    date_format: str,
    raw_df: pd.DataFrame,
) -> pd.DataFrame:
    usecols = [
        config.platform_code_col,
        config.derivative_name_col,
        config.date_col,
    ] + list(config.data_cols)
    raw_df = raw_df[usecols]

    raw_df = raw_df.assign(
        **{
            config.date_col: raw_df[config.date_col].map(
                lambda x: dt.datetime.strptime(x, date_format).replace(tzinfo=pytz.UTC)
            )
        }
    )

    raw_df = raw_df.rename(
        columns={
            **config.data_cols,
            **{
                config.platform_code_col: 'platform_code',
                config.date_col: 'event_dttm',
            },
        }
    )

    raw_df.platform_code = [x.strip() for x in raw_df.platform_code]

    platform_names = []
    derivative_names = []
    for derivative_dash_platform in raw_df[config.derivative_name_col]:
        derivative_name, platform_name = derivative_dash_platform.rsplit('-', 1)
        platform_names.append(platform_name.strip())
        derivative_names.append(derivative_name.strip())

    raw_df['platform_name'] = platform_names
    raw_df['derivative_name'] = derivative_names

    del raw_df[config.derivative_name_col]

    return raw_df


def insert_platforms_derivatives(
    conn: psycopg2.extensions.connection, config: DerivativeConfig, df: pd.DataFrame
):
    derivatives = set()
    platforms = {}
    for _, row in df.iterrows():
        derivatives.add((row.platform_code, row.derivative_name))
        platforms[row.platform_code] = row.platform_name

    platform_codes, platform_names = zip(*platforms.items())

    with conn.cursor() as cursor:
        cursor.execute(
            '''
            INSERT INTO validol_internal.cot_derivatives_platform (source, code, name)
            SELECT %s, UNNEST(%s), UNNEST(%s)
            ON CONFLICT (source, code) DO UPDATE SET
                name = EXCLUDED.name
            RETURNING id
        ''',
            (config.source, list(platform_codes), list(platform_names)),
        )
        platform_ids = dict(zip(platform_codes, pg.extract_ids_from_cursor(cursor)))

        derivative_platform_ids, derivative_names = zip(
            *[
                (platform_ids[platform_code], derivative_name)
                for platform_code, derivative_name in derivatives
            ]
        )
        cursor.execute(
            '''
            INSERT INTO validol_internal.cot_derivatives_info (cot_derivatives_platform_id, name)
            SELECT UNNEST(%s), UNNEST(%s)
            ON CONFLICT (cot_derivatives_platform_id, name) DO UPDATE SET
                name = EXCLUDED.name
            RETURNING id
        ''',
            (list(derivative_platform_ids), list(derivative_names)),
        )

        derivative_ids = dict(zip(derivatives, pg.extract_ids_from_cursor(cursor)))

    conn.commit()

    df['cot_derivatives_info_id'] = [
        derivative_ids[row.platform_code, row.derivative_name]
        for _, row in df.iterrows()
    ]


def insert_data(
    engine: sqlalchemy.engine.base.Engine, config: DerivativeConfig, df: pd.DataFrame
):
    for column in ['platform_code', 'platform_name', 'derivative_name']:
        del df[column]

    df['report_type'] = config.report_type

    df.to_sql(
        config.table_name,
        engine,
        schema='validol_internal',
        index=False,
        if_exists='append',
        method=pg.insert_on_conflict_do_nothing,
    )
