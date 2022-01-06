import collections

from aiohttp import web

from cloud_validol.loader.reports import refresh_views


async def handle(request: web.Request) -> web.Response:
    request_json = await request.json()

    queries = collections.defaultdict(list)
    for derivative in request_json['derivatives']:
        queries[derivative['source']].append(
            [derivative['series_id'], derivative['visible']]
        )

    async with request.app['pool'].acquire() as conn:
        for source in ['moex', 'cot']:
            if source not in queries:
                continue

            await conn.execute(
                f'''
                UPDATE validol_internal.{source}_derivatives_info AS t SET
                    visible = c.visible
                FROM (
                    SELECT 
                        UNNEST($1::BIGINT[]) AS series_id, 
                        UNNEST($2::BOOLEAN[]) AS visible
                ) AS c
                WHERE t.id = c.series_id
            ''',
                *map(list, zip(*queries[source])),
            )

        for view in refresh_views.VIEWS:
            await conn.execute(f'REFRESH MATERIALIZED VIEW {view}')

    return web.json_response({})
