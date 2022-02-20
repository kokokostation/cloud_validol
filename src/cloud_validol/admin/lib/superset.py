import aiohttp
import contextlib
import dataclasses
import json
from typing import Dict
from typing import List
from typing import Optional

from cloud_validol.lib import secdist


class BaseError(Exception):
    pass


class ColumnError(BaseError):
    def __init__(self, column_name):
        super().__init__()

        self.column_name = column_name


class BasicAtomCollision(ColumnError):
    pass


@contextlib.asynccontextmanager
async def superset_session() -> aiohttp.ClientSession:
    conn_data = secdist.get_superset_conn_data()
    base_url = conn_data['BASE_URL']

    async with aiohttp.ClientSession(base_url, raise_for_status=True) as session:
        response = await session.post(
            '/api/v1/security/login',
            json={
                'password': conn_data['PASSWORD'],
                'provider': 'db',
                'refresh': False,
                'username': conn_data['USER'],
            },
        )

        response_json = await response.json()
        token = response_json['access_token']

    async with aiohttp.ClientSession(
        base_url, headers={'Authorization': f'Bearer {token}'}, raise_for_status=True
    ) as session:
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
        print(column)
        columns.append(
            DatasetColumn(
                name=column['column_name'],
                expression=column.get('expression'),
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
