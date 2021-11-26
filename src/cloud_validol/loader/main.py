import logging

from cloud_validol.loader.lib import pg
from cloud_validol.loader.reports import prices
from cloud_validol.loader.reports import monetary


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(message)s',
        level=logging.DEBUG,
        datefmt='[%Y-%m-%d %H:%M:%S]'
    )

    engine = pg.get_engine()
    conn = pg.get_connection()

    prices.update(engine)
    monetary.update(engine, conn)
