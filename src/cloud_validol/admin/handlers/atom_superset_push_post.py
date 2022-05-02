import logging

from aiohttp import web
import marshmallow_dataclass as mdataclasses

from cloud_validol.admin.lib import superset
from cloud_validol.admin.lib.server import base as server_base


logger = logging.getLogger(__name__)


@mdataclasses.dataclass
class Request:
    name: str
    basic_atom_expression: str
    superset_dataset_id: int


async def handle(request: web.Request) -> web.Response:
    request_body = await server_base.deser_request_body(request, Request)

    async with superset.superset_session() as session:
        await superset.push_dataset_column(
            session=session,
            pk=request_body.superset_dataset_id,
            name=request_body.name,
            expression=request_body.basic_atom_expression,
        )

    return web.Response()
