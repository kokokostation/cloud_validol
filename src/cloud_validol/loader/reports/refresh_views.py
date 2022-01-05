import logging

import psycopg2
import sqlalchemy

logger = logging.getLogger(__name__)

VIEWS = [
    'validol.fredgraph',
    'validol.investing_prices',
    'validol.moex_derivatives',
    'validol.cot_futures_only',
    'validol.cot_disaggregated',
    'validol.cot_financial_futures',
]


def update(engine: sqlalchemy.engine.base.Engine, conn: psycopg2.extensions.connection):
    logger.info('Start refreshing materialized views')

    for view in VIEWS:
        with conn.cursor() as cursor:
            cursor.execute(f'REFRESH MATERIALIZED VIEW {view}')
        conn.commit()

        logger.info(f'Successfully refreshed {view}')

    logger.info('Finish refreshing materialized views')
