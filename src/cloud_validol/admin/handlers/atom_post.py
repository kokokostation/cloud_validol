import dataclasses
import logging

from aiohttp import web
import marshmallow_dataclass as mdataclasses

from cloud_validol.admin.lib import superset
from cloud_validol.admin.lib.atoms import grammar as atom_grammar
from cloud_validol.admin.lib.server import atoms as server_atoms
from cloud_validol.admin.lib.server import base as server_base


logger = logging.getLogger(__name__)


@mdataclasses.add_schema
@dataclasses.dataclass
class Request:
    name: str
    expression: str
    superset_dataset_id: int  # superset dataset to check expression against


async def handle(request: web.Request) -> web.Response:
    request_body = await server_base.deser_request_body(request, Request)

    try:
        atom_grammar.check_atom_name(request_body.name)
    except atom_grammar.ParseError as exc:
        raise web.HTTPBadRequest(reason=f'Bad atom name: {exc}')

    async with superset.superset_session() as session:
        dataset = await superset.get_dataset(session, request_body.superset_dataset_id)
    user_expressions = await server_atoms.get_user_expressions(request)

    dataset_columns = superset.parse_dataset_columns(
        dataset, user_atoms=list(user_expressions)
    )

    try:
        atom_grammar.get_stack(
            allow_unknown_atoms=False,
            library=atom_grammar.ExpressionLibrary(
                basic_atoms=dataset_columns.basic_atoms,
                user_expressions=user_expressions,
            ),
            expression=request_body.expression,
        )
    except atom_grammar.ParseError as exc:
        raise web.HTTPBadRequest(reason=f'Failed to parse expression: {exc}')

    logger.info('Expression passed checks, writing to db')

    await server_atoms.insert_user_expression(
        request, request_body.name, request_body.expression
    )

    return web.Response()
