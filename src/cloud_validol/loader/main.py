import logging

from cloud_validol.loader.lib import pg
from cloud_validol.loader.reports import prices


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(message)s',
        level=logging.DEBUG,
        datefmt='[%Y-%m-%d %H:%M:%S]'
    )

    engine = pg.get_engine()

    prices.update(engine)
