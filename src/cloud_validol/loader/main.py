import logging

import click

from cloud_validol.loader.lib import pg
from cloud_validol.loader.reports import prices
from cloud_validol.loader.reports import monetary
from cloud_validol.loader.reports import moex
from cloud_validol.loader.reports import cftc
from cloud_validol.loader.reports import ice

logger = logging.getLogger(__name__)

UPDATE_SOURCES = {
    'prices': prices.update,
    'monetary': monetary.update,
    'moex': moex.update,
    'cftc': cftc.update,
    'ice': ice.update,
}


@click.command()
@click.option('--source', '-s', multiple=True, default=UPDATE_SOURCES.keys())
def main(source):
    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(message)s',
        level=logging.DEBUG,
        datefmt='[%Y-%m-%d %H:%M:%S]',
    )

    engine = pg.get_engine()

    with pg.get_connection() as conn:
        for s in source:
            if s in UPDATE_SOURCES:
                UPDATE_SOURCES[s](engine, conn)
            else:
                logger.error('Passed nonexistent source to cli: %s', s)


if __name__ == '__main__':
    main()
