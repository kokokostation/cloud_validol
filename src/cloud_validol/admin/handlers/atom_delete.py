import logging

from aiohttp import web

from cloud_validol.admin.lib.atoms import grammar as atom_grammar
from cloud_validol.admin.lib.server import atoms as server_atoms


logger = logging.getLogger(__name__)


async def handle(request: web.Request) -> web.Response:
    atom_name = request.query['name']

    user_expressions = await server_atoms.get_user_expressions(request)
    atom_graph = atom_grammar.build_atom_graph(user_expressions)

    if atom_name not in atom_graph:
        raise web.HTTPNotFound(reason=f'Atom \'{atom_name}\' not found')

    in_nodes = [source for source, target in atom_graph.in_edges(atom_name)]

    if len(in_nodes) > 0:
        raise web.HTTPBadRequest(
            reason=f'Can\'t delete atom, some other atoms depend on it: {in_nodes}'
        )

    await server_atoms.delete_user_expression(request, atom_name)

    return web.Response()
