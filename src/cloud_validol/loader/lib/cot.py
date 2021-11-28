import dataclasses
import datetime as dt
import logging
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd
import sqlalchemy

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DerivativeConfig:
    name: str
    table_name: str
    platform_code_col: str
    derivative_name_col: str
    date_col: str
    date_format: str
    data_cols: Dict[str, str]
    year_download_url: str
    global_from_year: int
    report_type: str
    initial_download_url: Optional[str] = None
    initial_date_format: Optional[str] = None


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
                MAX(DATE(event_dttm)) AS last_event_dt
            FROM validol_internal.{config.table_name}
            WHERE report_type = %(report_type)s
        ''',
        engine,
        params={'report_type': config.report_type},
    )

    last_event_dt = df.iloc[0].last_event_dt
    today = dt.date.today()

    if last_event_dt is None:
        logger.info('No data for %s, downloading from scratch', config.name)

        return UpdateInterval(
            load_initial=True,
            years_to_load=list(range(config.global_from_year, today.year + 1)),
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
