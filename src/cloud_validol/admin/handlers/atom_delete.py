import dataclasses
import logging

from aiohttp import web
import marshmallow_dataclass as mdataclasses

from cloud_validol.admin.lib.atoms import grammar as atom_grammar
from cloud_validol.admin.lib.server import atoms as server_atoms
from cloud_validol.admin.lib.server import base as server_base


logger = logging.getLogger(__name__)


@mdataclasses.add_schema
@dataclasses.dataclass
class Request:
    name: str


async def handle(request: web.Request) -> web.Response:
    request_body = await server_base.deser_request_body(request, Request)
    user_expressions = await server_atoms.get_user_expressions(request)
    atom_graph = atom_grammar.build_atom_graph(user_expressions)

    in_nodes = [source for source, target in atom_graph.in_edges(request_body.name)]

    if len(in_nodes) > 0:
        raise web.HTTPBadRequest(
            reason=f'Can\'t delete atom, some other atoms depend on it: {in_nodes}'
        )

    await server_atoms.delete_user_expression(request, request_body.name)

    return web.Response()
