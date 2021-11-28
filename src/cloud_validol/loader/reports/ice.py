import copy
import dataclasses
import io
import logging
from typing import List

import pandas as pd
import psycopg2
import requests
import sqlalchemy

from cloud_validol.loader.lib import cot

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class IceDownloadConfig:
    report_type_filter: str


def _make_derivative_configs() -> List[cot.DerivativeConfig]:
    ice_disaggregated_futures_only = cot.DerivativeConfig(
        name='ice_disaggregated_futures_only',
        source='ice',
        table_name='cot_disaggregated_data',
        platform_code_col='CFTC_Market_Code',
        derivative_name_col='Market_and_Exchange_Names',
        date_col='As_of_Date_Form_MM/DD/YYYY',
        date_format='%m/%d/%Y',
        data_cols={
            'Open_Interest_All': 'oi',
            'Prod_Merc_Positions_Long_All': 'pmpl',
            'Prod_Merc_Positions_Short_All': 'pmps',
            'Swap_Positions_Long_All': 'sdpl',
            'Swap_Positions_Short_All': 'sdps',
            'M_Money_Positions_Long_All': 'mmpl',
            'M_Money_Positions_Short_All': 'mmps',
            'Other_Rept_Positions_Long_All': 'orpl',
            'Other_Rept_Positions_Short_All': 'orps',
            'NonRept_Positions_Long_All': 'nrl',
            'NonRept_Positions_Short_All': 'nrs',
            'Conc_Gross_LE_4_TDR_Long_All': '4gl%',
            'Conc_Gross_LE_4_TDR_Short_All': '4gs%',
            'Conc_Gross_LE_8_TDR_Long_All': '8gl%',
            'Conc_Gross_LE_8_TDR_Short_All': '8gs%',
            'Conc_Net_LE_4_TDR_Long_All': '4l%',
            'Conc_Net_LE_4_TDR_Short_All': '4s%',
            'Conc_Net_LE_8_TDR_Long_All': '8l%',
            'Conc_Net_LE_8_TDR_Short_All': '8s%',
            'Swap_Positions_Spread_All': 'sdp_spr',
            'M_Money_Positions_Spread_All': 'mmp_spr',
            'Other_Rept_Positions_Spread_All': 'orp_spr',
        },
        initial_from_year=2011,
        report_type='futures_only',
        download_config=IceDownloadConfig(report_type_filter='FutOnly'),
    )

    ice_disaggregated_combined = copy.deepcopy(ice_disaggregated_futures_only)
    ice_disaggregated_combined.name = 'ice_disaggregated_combined'
    ice_disaggregated_combined.download_config.report_type_filter = 'Combined'
    ice_disaggregated_combined.report_type = 'combined'

    return [
        ice_disaggregated_futures_only,
        ice_disaggregated_combined,
    ]


def _download_doc(
    year: int,
) -> pd.DataFrame:
    logger.info('Downloading %s for ICE', year)

    response = requests.get(
        'https://www.theice.com/publicdocs/futures/COTHist{year}.csv'.format(year=year),
        headers={'User-Agent': 'Mozilla/5.0'},
    )

    df = pd.read_csv(io.StringIO(response.text))
    df = df.rename(columns={
        'Swap__Positions_Short_All': 'Swap_Positions_Short_All',
        'Swap__Positions_Spread_All': 'Swap_Positions_Spread_All',
    })

    return df


def update(engine: sqlalchemy.engine.base.Engine, conn: psycopg2.extensions.connection):
    logger.info('Start updating ICE data')

    configs = _make_derivative_configs()

    years_to_load = []
    for config in configs:
        update_interval = cot.get_interval(engine, config)

        if update_interval is None:
            continue

        years_to_load.extend(update_interval.years_to_load)

    overall_dfs = []
    for year in set(years_to_load):
        overall_dfs.append(_download_doc(year))

    overall_df = pd.concat(overall_dfs)
    overall_df = overall_df.replace('#VALUE!', None)

    for config in configs:
        raw_df = overall_df[
            overall_df['FutOnly_or_Combined']
            == config.download_config.report_type_filter
        ]
        df = cot.process_raw_dataframe(config, config.date_format, raw_df)

        cot.insert_platforms_derivatives(conn, config, df)
        cot.insert_data(engine, config, df)

    logger.info('Finish updating ICE data')
