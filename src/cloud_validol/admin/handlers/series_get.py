from aiohttp import web


async def handle(request: web.Request) -> web.Response:
    derivatives = []
    async with request.app['pool'].acquire() as conn:
        rows = await conn.fetch(
            '''
            SELECT 
                series_id,
                platform_source,
                platform_code,
                derivative_name,
                visible
            FROM validol_interface.cot_derivatives_index
        '''
        )
        derivatives.extend(
            {
                'name': {
                    'platform_source': row['platform_source'],
                    'platform_code': row['platform_code'],
                    'derivative_name': row['derivative_name'],
                },
                'info': {
                    'source': 'cot',
                    'series_id': row['series_id'],
                    'visible': row['visible'],
                },
            }
            for row in rows
        )

        rows = await conn.fetch(
            '''
            SELECT
                series_id,
                derivative_name,
                visible
            FROM validol_interface.moex_derivatives_index
        '''
        )
        derivatives.extend(
            {
                'name': {
                    'platform_source': 'moex',
                    'platform_code': 'moex',
                    'derivative_name': row['derivative_name'],
                },
                'info': {
                    'source': 'moex',
                    'series_id': row['series_id'],
                    'visible': row['visible'],
                },
            }
            for row in rows
        )

    return web.json_response({'derivatives': derivatives})
