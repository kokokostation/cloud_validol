import logging

from aiohttp import web

from cloud_validol.admin.handlers import atom_delete
from cloud_validol.admin.handlers import atom_post
from cloud_validol.admin.handlers import atom_put
from cloud_validol.admin.handlers import atoms_superset_push_post
from cloud_validol.admin.handlers import atoms_get
from cloud_validol.admin.handlers import investing_prices_get
from cloud_validol.admin.handlers import investing_prices_put
from cloud_validol.admin.handlers import series_get
from cloud_validol.admin.handlers import series_update_start_post
from cloud_validol.admin.handlers import series_update_poll_get
from cloud_validol.admin.lib import pg
from cloud_validol.admin.lib.server import jobs


logging.basicConfig(level=logging.DEBUG)


async def init_app():
    app = web.Application()

    app['pool'] = await pg.get_connection_pool()

    app.add_routes(
        [
            web.get('/investing_prices', investing_prices_get.handle),
            web.put('/investing_prices', investing_prices_put.handle),
            web.get('/series', series_get.handle),
            web.post('/series/update_start', series_update_start_post.handle),
            web.get('/series/update_poll', series_update_poll_get.handle),
            web.get('/atoms', atoms_get.handle),
            web.delete('/atom', atom_delete.handle),
            web.post('/atom', atom_post.handle),
            web.put('/atom', atom_put.handle),
            web.post('/atoms/superset_push', atoms_superset_push_post.handle),
        ]
    )

    jobs.setup(app)

    return app


def main():
    web.run_app(init_app())


if __name__ == '__main__':
    main()
