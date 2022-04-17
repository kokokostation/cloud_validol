from aiohttp import web
import asyncio
import dataclasses
import enum
import logging
from typing import Dict
from typing import List
from typing import Optional

from cloud_validol.admin.lib.atoms import grammar as atom_grammar
from cloud_validol.admin.lib.server import superset as server_superset
from cloud_validol.admin.lib import superset


logger = logging.getLogger(__name__)


class ColumnState(enum.Enum):
    BASIC = 'BASIC'
    IN_SYNC = 'IN_SYNC'
    SYNC_NEEDED = 'SYNC_NEEDED'
    SUPERSET_ONLY = 'SUPERSET_ONLY'
    VALIDOL_ONLY = 'VALIDOL_ONLY'


@dataclasses.dataclass(frozen=True)
class UserExpression:
    expression: str
    basic_atoms_expression: str


@dataclasses.dataclass(frozen=True)
class SupersetExpression:
    expression: str


@dataclasses.dataclass(frozen=True)
class Column:
    name: str
    state: str
    user_expression: Optional[UserExpression] = None
    superset_expression: Optional[SupersetExpression] = None


@dataclasses.dataclass(frozen=True)
class Dataset:
    superset_id: int
    name: str
    columns: List[Column]


@dataclasses.dataclass(frozen=True)
class Atom:
    name: str
    expression: str


@dataclasses.dataclass(frozen=True)
class Response:
    datasets: List[Dataset]
    atoms: List[Atom]


async def _get_datasets() -> List[superset.DatasetItemView]:
    async with superset.superset_session() as session:
        superset_datasets = await superset.get_datasets(session)

        return await asyncio.gather(
            *[
                superset.get_dataset(session, dataset.id)
                for dataset in superset_datasets
            ]
        )


async def _get_user_expressions(request: web.Request) -> Dict[str, str]:
    async with request.app['pool'].acquire() as conn:
        rows = await conn.fetch(
            '''
            SELECT 
                name,
                expression
            FROM validol_internal.atom
        '''
        )

    return {row['name']: row['expression'] for row in rows}


def _get_basic_atom_expressions(
    basic_atoms: List[str], user_expressions: Dict[str, str]
) -> Dict[str, str]:
    if not user_expressions:
        return {}

    atom_expressions: Dict[str, Optional[str]] = {
        **{atom: None for atom in basic_atoms},
        **user_expressions,
    }
    user_atoms = list(user_expressions.keys())
    user_atom_expressions = list(user_expressions.values())
    user_atom_stacks = atom_grammar.get_stacks(user_atom_expressions, atom_expressions)

    return {
        atom: atom_grammar.render_stack(stack)
        for atom, stack in zip(user_atoms, user_atom_stacks)
    }


def _make_response_dataset(
    superset_dataset: superset.DatasetItemView, user_expressions: Dict[str, str]
) -> Dataset:
    superset_columns_info = server_superset.parse_dataset_columns(
        superset_dataset, list(user_expressions)
    )
    basic_atom_expressions = _get_basic_atom_expressions(
        superset_columns_info.basic_atoms, user_expressions
    )

    result_columns = []
    for basic_atom in superset_columns_info.basic_atoms:
        result_columns.append(Column(name=basic_atom, state=ColumnState.BASIC.value))

    for atom, user_expression_str in user_expressions.items():
        superset_expression_str = superset_columns_info.expressions.get(atom)
        if superset_expression_str is not None:
            superset_expression = SupersetExpression(expression=superset_expression_str)
            if superset_expression_str == user_expression_str:
                state = ColumnState.IN_SYNC.value
            else:
                state = ColumnState.SYNC_NEEDED.value
        else:
            superset_expression = None
            state = ColumnState.VALIDOL_ONLY.value

        result_columns.append(
            Column(
                name=atom,
                state=state,
                user_expression=UserExpression(
                    expression=user_expression_str,
                    basic_atoms_expression=basic_atom_expressions[atom],
                ),
                superset_expression=superset_expression,
            )
        )

    for atom, superset_expression_str in superset_columns_info.expressions.items():
        if atom not in user_expressions:
            result_columns.append(
                Column(
                    name=atom,
                    state=ColumnState.SUPERSET_ONLY.value,
                    superset_expression=SupersetExpression(
                        expression=superset_expression_str
                    ),
                )
            )

    return Dataset(
        superset_id=superset_dataset.id,
        name=superset_dataset.table_name,
        columns=result_columns,
    )


async def handle(request: web.Request) -> web.Response:
    superset_datasets = await _get_datasets()
    user_expressions = await _get_user_expressions(request)

    response_datasets = []
    for superset_dataset in superset_datasets:
        response_datasets.append(
            _make_response_dataset(superset_dataset, user_expressions)
        )

    response_atoms = [
        Atom(name=name, expression=expression)
        for name, expression in user_expressions.items()
    ]

    return web.json_response(
        dataclasses.asdict(Response(datasets=response_datasets, atoms=response_atoms))
    )
