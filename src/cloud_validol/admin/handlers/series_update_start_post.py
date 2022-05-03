import collections
import dataclasses
import logging
from typing import List

from aiohttp import web
import marshmallow_dataclass as mdataclasses

from cloud_validol.admin.lib.server import base as server_base
from cloud_validol.admin.lib.server import jobs
from cloud_validol.loader.reports import refresh_views


logger = logging.getLogger(__name__)


@mdataclasses.add_schema
@dataclasses.dataclass
class Derivative:
    source: str
    series_id: int
    visible: bool


@mdataclasses.add_schema
@dataclasses.dataclass
class Request:
    derivatives: List[Derivative]


@mdataclasses.add_schema
@dataclasses.dataclass
class Response:
    job_id: str


async def job(request: web.Request) -> None:
    logger.info('Start refreshing materialized views')

    async with request.app['pool'].acquire() as conn:
        for view in refresh_views.VIEWS:
            await conn.execute(f'REFRESH MATERIALIZED VIEW {view}')

    logger.info('Finished refreshing materialized views')


async def handle(request: web.Request) -> web.Response:
    request_body = await server_base.deser_request_body(request, Request)

    queries = collections.defaultdict(list)
    for derivative in request_body.derivatives:
        queries[derivative.source].append([derivative.series_id, derivative.visible])

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

    job_id = await jobs.spawn(request, job(request))

    return web.json_response(server_base.ser_response_body(Response(job_id=job_id)))
