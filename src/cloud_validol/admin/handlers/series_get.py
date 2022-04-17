import dataclasses
from typing import List

from aiohttp import web


@dataclasses.dataclass(frozen=True)
class DerivativeName:
    platform_source: str
    platform_code: str
    derivative_name: str


@dataclasses.dataclass(frozen=True)
class DerivativeInfo:
    source: str
    series_id: int
    visible: bool


@dataclasses.dataclass(frozen=True)
class Derivative:
    name: DerivativeName
    info: DerivativeInfo


@dataclasses.dataclass(frozen=True)
class Response:
    derivatives: List[Derivative]


async def handle(request: web.Request) -> web.Response:
    derivatives: List[Derivative] = []
    async with request.app['pool'].acquire() as conn:
        rows = await conn.fetch(
            '''
            SELECT 
                series_id,
                platform_source,
                platform_code,
                derivative_name,
                visible
            FROM validol_interface.cot_derivatives_index
        '''
        )
        derivatives.extend(
            Derivative(
                name=DerivativeName(
                    platform_source=row['platform_source'],
                    platform_code=row['platform_code'],
                    derivative_name=row['derivative_name'],
                ),
                info=DerivativeInfo(
                    source='cot',
                    series_id=row['series_id'],
                    visible=row['visible'],
                ),
            )
            for row in rows
        )

        rows = await conn.fetch(
            '''
            SELECT
                series_id,
                derivative_name,
                visible
            FROM validol_interface.moex_derivatives_index
        '''
        )
        derivatives.extend(
            Derivative(
                name=DerivativeName(
                    platform_source='moex',
                    platform_code='moex',
                    derivative_name=row['derivative_name'],
                ),
                info=DerivativeInfo(
                    source='moex',
                    series_id=row['series_id'],
                    visible=row['visible'],
                ),
            )
            for row in rows
        )

    return web.json_response(dataclasses.asdict(Response(derivatives=derivatives)))
