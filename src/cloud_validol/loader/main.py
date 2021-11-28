import logging

from cloud_validol.loader.lib import pg
from cloud_validol.loader.reports import prices
from cloud_validol.loader.reports import monetary
from cloud_validol.loader.reports import moex
from cloud_validol.loader.reports import cftc


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(message)s',
        level=logging.DEBUG,
        datefmt='[%Y-%m-%d %H:%M:%S]',
    )

    engine = pg.get_engine()

    with pg.get_connection() as conn:
        prices.update(engine, conn)
        monetary.update(engine, conn)
        moex.update(engine, conn)
        cftc.update(engine, conn)
