import dataclasses
import logging
from typing import List

from aiohttp import web
import marshmallow_dataclass as mdataclasses

from cloud_validol.admin.lib import superset
from cloud_validol.admin.lib.server import base as server_base


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class PushColumn:
    name: str
    basic_atoms_expression: str


@mdataclasses.add_schema
@dataclasses.dataclass
class Request:
    superset_dataset_id: int
    columns: List[PushColumn]


async def handle(request: web.Request) -> web.Response:
    request_body = await server_base.deser_request_body(request, Request)

    async with superset.superset_session() as session:
        await superset.push_dataset_columns(
            session=session,
            pk=request_body.superset_dataset_id,
            columns=[
                superset.ColumnExpression(
                    name=column.name,
                    expression=column.basic_atoms_expression,
                )
                for column in request_body.columns
            ],
        )

    return web.Response()
