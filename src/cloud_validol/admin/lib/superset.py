import contextlib
import copy
import dataclasses
import json
import logging
from typing import AsyncGenerator
from typing import Dict
from typing import List
from typing import Optional

import aiohttp

from cloud_validol.lib import secdist


logger = logging.getLogger(__name__)


class BaseError(Exception):
    pass


class ColumnError(BaseError):
    def __init__(self, column_name):
        super().__init__()

        self.column_name = column_name


class BasicAtomCollision(ColumnError):
    pass


@contextlib.asynccontextmanager
async def superset_session() -> AsyncGenerator[aiohttp.ClientSession, None]:
    conn_data = secdist.get_superset_conn_data()

    async with aiohttp.ClientSession(
        conn_data.base_url, raise_for_status=True
    ) as session:
        response = await session.post(
            '/api/v1/security/login',
            json={
                'password': conn_data.password,
                'provider': 'db',
                'refresh': False,
                'username': conn_data.user,
            },
        )

        response_json = await response.json()
        token = response_json['access_token']

    async with aiohttp.ClientSession(
        conn_data.base_url,
        headers={'Authorization': f'Bearer {token}'},
        raise_for_status=True,
    ) as session:
        response = await session.get('/api/v1/security/csrf_token/')
        response_json = await response.json()
        session.headers['X-CSRFToken'] = response_json['result']

        yield session


@dataclasses.dataclass(frozen=True)
class DatasetListView:
    id: int


@dataclasses.dataclass(frozen=True)
class DatasetColumn:
    name: str
    expression: Optional[str]
    is_dimension: bool


@dataclasses.dataclass(frozen=True)
class DatasetItemView:
    id: int
    table_name: str
    columns: List[DatasetColumn]


@dataclasses.dataclass(frozen=True)
class DatasetColumnsInfo:
    basic_atoms: List[str]
    expressions: Dict[str, str]


@dataclasses.dataclass(frozen=True)
class ColumnExpression:
    name: str
    expression: str


async def get_datasets(session: aiohttp.ClientSession) -> List[DatasetListView]:
    query = json.dumps({'page': 0, 'page_size': 100})
    response = await session.get('/api/v1/dataset/', params={'q': query})
    response_json = await response.json()

    result = []
    for dataset in response_json['result']:
        result.append(DatasetListView(id=dataset['id']))

    return result


async def get_dataset(session: aiohttp.ClientSession, pk: int) -> DatasetItemView:
    response = await session.get(f'/api/v1/dataset/{pk}')
    response_json = await response.json()
    response_result = response_json['result']

    columns = []
    for column in response_result['columns']:
        columns.append(
            DatasetColumn(
                name=column['column_name'],
                expression=column.get('expression') or None,
                is_dimension=column.get('groupby', True),
            )
        )

    return DatasetItemView(
        id=response_result['id'],
        table_name=response_result['table_name'],
        columns=columns,
    )


def parse_dataset_columns(
    dataset: DatasetItemView, user_atoms: List[str]
) -> DatasetColumnsInfo:
    basic_atoms: List[str] = []
    expressions: Dict[str, str] = {}

    for column in dataset.columns:
        if not column.is_dimension:
            if column.expression is None:
                if column.name in user_atoms:
                    raise BasicAtomCollision(column.name)

                basic_atoms.append(column.name)
            else:
                expressions[column.name] = column.expression

    return DatasetColumnsInfo(basic_atoms=basic_atoms, expressions=expressions)


async def push_dataset_columns(
    session: aiohttp.ClientSession,
    pk: int,
    columns: List[ColumnExpression],
) -> None:
    response = await session.get(f'/api/v1/dataset/{pk}')
    response_json = await response.json()

    update_request = copy.deepcopy(response_json['result'])

    update_request['database_id'] = update_request['database']['id']
    del update_request['database']
    update_request.pop('datasource_type', None)
    update_request.pop('id', None)
    update_request.pop('url', None)
    for column in update_request['columns']:
        column['is_active'] = True
        column.pop('id')
        column.pop('uuid')
        column.pop('changed_on', None)
        column.pop('created_on', None)
        column.pop('type_generic', None)
    for metric in update_request['metrics']:
        metric.pop('changed_on', None)
        metric.pop('created_on', None)
    update_request['owners'] = [owner['id'] for owner in update_request['owners']]

    for column in columns:
        push_column_data = {
            'column_name': column.name,
            'expression': column.expression,
            'filterable': True,
            'groupby': False,
            'is_active': True,
            'is_dttm': False,
            'type': 'NUMERIC',
        }
        pushed_column = None
        for request_column in update_request['columns']:
            if request_column['column_name'] == column.name:
                pushed_column = request_column
                break

        if pushed_column is not None:
            pushed_column.update(**push_column_data)
        else:
            update_request['columns'].append(push_column_data)

    reset_request = copy.deepcopy(update_request)
    reset_request['columns'] = [
        {
            'column_name': '__technical_column__',
            'expression': '1',
            'filterable': True,
            'groupby': False,
            'is_active': True,
            'is_dttm': False,
            'type': 'NUMERIC',
        }
    ]

    await session.put(
        f'/api/v1/dataset/{pk}',
        json=reset_request,
    )

    await session.put(
        f'/api/v1/dataset/{pk}',
        json=update_request,
    )
