import copy
import datetime as dt
import io
import logging
from typing import List
import zipfile

import pandas as pd
import psycopg2
import pytz
import requests
import sqlalchemy

from cloud_validol.loader.lib import cot
from cloud_validol.loader.lib import pg

logger = logging.getLogger(__name__)


def _make_derivative_configs() -> List[cot.DerivativeConfig]:
    cftc_date_format = '%Y-%m-%d'

    cftc_disaggregated_futures_only = cot.DerivativeConfig(
        name='cftc_disaggregated_futures_only',
        table_name='cot_disaggregated_data',
        platform_code_col='CFTC_Market_Code',
        derivative_name_col='Market_and_Exchange_Names',
        date_col='Report_Date_as_YYYY-MM-DD',
        date_format=cftc_date_format,
        data_cols={
            'Open_Interest_All': 'oi',
            'Prod_Merc_Positions_Long_All': 'pmpl',
            'Prod_Merc_Positions_Short_All': 'pmps',
            'Swap_Positions_Long_All': 'sdpl',
            'Swap__Positions_Short_All': 'sdps',
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
            'Swap__Positions_Spread_All': 'sdps_pr',
            'M_Money_Positions_Spread_All': 'mmps_pr',
            'Other_Rept_Positions_Spread_All': 'orps_pr',
        },
        global_from_year=2017,
        report_type='futures_only',
        year_download_url='http://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip',
        initial_download_url='http://www.cftc.gov/files/dea/history/fut_disagg_txt_hist_2006_2016.zip',
    )

    cftc_disaggregated_combined = copy.deepcopy(cftc_disaggregated_futures_only)
    cftc_disaggregated_combined.name = 'cftc_disaggregated_combined'
    cftc_disaggregated_combined.year_download_url = (
        'http://www.cftc.gov/files/dea/history/com_disagg_txt_{year}.zip'
    )
    cftc_disaggregated_combined.initial_download_url = (
        'http://www.cftc.gov/files/dea/history/com_disagg_txt_hist_2006_2016.zip'
    )
    cftc_disaggregated_combined.report_type = 'combined'

    cftc_financial_futures_only = cot.DerivativeConfig(
        name='cftc_financial_futures_only',
        table_name='cot_financial_futures_data',
        platform_code_col='CFTC_Market_Code',
        derivative_name_col='Market_and_Exchange_Names',
        date_col='Report_Date_as_YYYY-MM-DD',
        date_format=cftc_date_format,
        data_cols={
            'Open_Interest_All': 'oi',
            'Dealer_Positions_Long_All': 'dipl',
            'Dealer_Positions_Short_All': 'dips',
            'Dealer_Positions_Spread_All': 'dips_pr',
            'Asset_Mgr_Positions_Long_All': 'ampl',
            'Asset_Mgr_Positions_Short_All': 'amps',
            'Asset_Mgr_Positions_Spread_All': 'amps_pr',
            'Lev_Money_Positions_Long_All': 'lmpl',
            'Lev_Money_Positions_Short_All': 'lmps',
            'Lev_Money_Positions_Spread_All': 'lmps_pr',
            'Other_Rept_Positions_Long_All': 'orpl',
            'Other_Rept_Positions_Short_All': 'orps',
            'Other_Rept_Positions_Spread_All': 'orps_pr',
            'NonRept_Positions_Long_All': 'nrl',
            'NonRept_Positions_Short_All': 'nrs',
        },
        global_from_year=2017,
        report_type='futures_only',
        year_download_url='http://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip',
        initial_download_url='http://www.cftc.gov/files/dea/history/fin_fut_txt_2006_2016.zip',
        initial_date_format='%m/%d/%Y 12:00:00 AM',
    )

    cftc_financial_combined = copy.deepcopy(cftc_financial_futures_only)
    cftc_financial_combined.name = 'cftc_financial_combined'
    cftc_financial_combined.year_download_url = (
        'http://www.cftc.gov/files/dea/history/com_fin_txt_{year}.zip'
    )
    cftc_financial_combined.initial_download_url = (
        'http://www.cftc.gov/files/dea/history/fin_com_txt_2006_2016.zip'
    )
    cftc_financial_combined.report_type = 'combined'

    cftc_futures_only = cot.DerivativeConfig(
        name='cftc_futures_only',
        table_name='cot_futures_only_data',
        platform_code_col='CFTC Market Code in Initials',
        derivative_name_col='Market and Exchange Names',
        date_col='As of Date in Form YYYY-MM-DD',
        date_format='%Y-%m-%d',
        data_cols={
            'Open Interest (All)': 'oi',
            'Noncommercial Positions-Long (All)': 'ncl',
            'Noncommercial Positions-Short (All)': 'ncs',
            'Commercial Positions-Long (All)': 'cl',
            'Commercial Positions-Short (All)': 'cs',
            'Nonreportable Positions-Long (All)': 'nrl',
            'Nonreportable Positions-Short (All)': 'nrs',
            'Concentration-Net LT =4 TDR-Long (All)': '4l%',
            'Concentration-Net LT =4 TDR-Short (All)': '4s%',
            'Concentration-Net LT =8 TDR-Long (All)': '8l%',
            'Concentration-Net LT =8 TDR-Short (All)': '8s%',
        },
        global_from_year=2017,
        report_type='futures_only',
        year_download_url='http://www.cftc.gov/files/dea/history/deacot{year}.zip',
        initial_download_url='http://www.cftc.gov/files/dea/history/deacot1986_2016.zip',
    )

    return [
        cftc_futures_only,
        cftc_disaggregated_futures_only,
        cftc_disaggregated_combined,
        cftc_financial_futures_only,
        cftc_financial_combined,
    ]


def _download_doc(
    config: cot.DerivativeConfig, url: str, date_format: str
) -> pd.DataFrame:
    logger.info('Downloading %s for %s', url, config.name)

    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})

    with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_file:
        path = zip_file.namelist()[0]
        csv_buff = zip_file.read(path).decode('utf-8')

    usecols = [
        config.platform_code_col,
        config.derivative_name_col,
        config.date_col,
    ] + list(config.data_cols)
    df = pd.read_csv(
        io.StringIO(csv_buff),
        usecols=usecols,
        parse_dates=[config.date_col],
        date_parser=lambda x: dt.datetime.strptime(x, date_format).replace(
            tzinfo=pytz.UTC
        ),
    )

    df = df.rename(
        columns={
            **config.data_cols,
            **{
                config.platform_code_col: 'platform_code',
                config.date_col: 'event_dttm',
            },
        }
    )

    df.platform_code = [x.strip() for x in df.platform_code]

    platform_names = []
    derivative_names = []
    for derivative_dash_platform in df[config.derivative_name_col]:
        derivative_name, platform_name = derivative_dash_platform.rsplit('-', 1)
        platform_names.append(platform_name.strip())
        derivative_names.append(derivative_name.strip())

    df['platform_name'] = platform_names
    df['derivative_name'] = derivative_names

    del df[config.derivative_name_col]

    return df


def _insert_platforms_derivatives(
    conn: psycopg2.extensions.connection, df: pd.DataFrame
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
            INSERT INTO validol_internal.cot_derivatives_platform (code, name)
            SELECT UNNEST(%s), UNNEST(%s)
            ON CONFLICT (code) DO UPDATE SET
                name = EXCLUDED.name
            RETURNING id
        ''',
            (list(platform_codes), list(platform_names)),
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


def update(engine: sqlalchemy.engine.base.Engine, conn: psycopg2.extensions.connection):
    logger.info('Start updating CFTC data')

    for config in _make_derivative_configs():
        update_interval = cot.get_interval(engine, config)

        if update_interval is None:
            continue

        dfs = []
        if update_interval.load_initial:
            dfs.append(
                _download_doc(
                    config,
                    config.initial_download_url,
                    config.initial_date_format or config.date_format,
                )
            )

        for year in update_interval.years_to_load:
            dfs.append(
                _download_doc(
                    config,
                    config.year_download_url.format(year=year),
                    config.date_format,
                )
            )

        df = pd.concat(dfs)

        _insert_platforms_derivatives(conn, df)

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

    logger.info('Finish updating CFTC data')
