from loader.lib import pg
from loader.reports import prices


def main():
    engine = pg.get_engine()

    prices.update(engine)
