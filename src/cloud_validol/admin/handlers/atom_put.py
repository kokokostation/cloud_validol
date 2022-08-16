import dataclasses
import logging

from aiohttp import web
import marshmallow_dataclass as mdataclasses
import networkx as nx

from cloud_validol.admin.lib.atoms import grammar as atom_grammar
from cloud_validol.admin.lib.server import atoms as server_atoms
from cloud_validol.admin.lib.server import base as server_base


logger = logging.getLogger(__name__)


@mdataclasses.add_schema
@dataclasses.dataclass
class Request:
    name: str
    expression: str


async def handle(request: web.Request) -> web.Response:
    request_body = await server_base.deser_request_body(request, Request)
    user_expressions = await server_atoms.get_user_expressions(request)
    atom_graph = atom_grammar.build_atom_graph(user_expressions)

    if request_body.name not in atom_graph:
        raise web.HTTPNotFound(reason=f'Atom \'{request_body.name}\' not found')

    current_deps = {
        atom
        for atom in nx.descendants(atom_graph, request_body.name)
        if len(atom_graph.out_edges(atom)) == 0
    }
    expression_deps = atom_grammar.get_dependencies(
        atom_grammar.get_stack(
            allow_unknown_atoms=True,
            expression=request_body.expression,
            library=atom_grammar.ExpressionLibrary(
                user_expressions=user_expressions,
                basic_atoms=[],
            ),
        )
    )

    if not expression_deps.issubset(current_deps):
        raise web.HTTPBadRequest(
            reason=(
                f'Can\'t update atom, new expression dependencies: {expression_deps} '
                f'are not a subset of current ones: {current_deps}'
            )
        )

    await server_atoms.update_user_expression(
        request, request_body.name, request_body.expression
    )

    return web.Response()
