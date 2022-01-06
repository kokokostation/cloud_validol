import logging

from aiohttp import web

from cloud_validol.admin.handlers import investing_prices_get
from cloud_validol.admin.handlers import investing_prices_put
from cloud_validol.admin.handlers import series_get
from cloud_validol.admin.handlers import series_put
from cloud_validol.admin.lib import pg


logging.basicConfig(level=logging.DEBUG)


async def init_app():
    app = web.Application()

    app['pool'] = await pg.get_connection_pool()

    app.add_routes(
        [
            web.get('/investing_prices', investing_prices_get.handle),
            web.put('/investing_prices', investing_prices_put.handle),
            web.get('/series', series_get.handle),
            web.put('/series', series_put.handle),
        ]
    )

    return app


def main():
    web.run_app(init_app())


if __name__ == '__main__':
    main()
