import dataclasses

from aiohttp import web
import marshmallow_dataclass as mdataclasses

from cloud_validol.admin.lib.server import base as server_base
from cloud_validol.admin.lib.server import jobs


@mdataclasses.add_schema
@dataclasses.dataclass
class Response:
    job_info: jobs.JobInfo


async def handle(request: web.Request) -> web.Response:
    job_info = jobs.get_job_info(request, request.query['job_id'])
    if job_info is None:
        raise web.HTTPNotFound

    return web.json_response(server_base.ser_response_body(Response(job_info=job_info)))
