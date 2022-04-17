import dataclasses

from aiohttp import web


@dataclasses.dataclass(frozen=True)
class Request:
    name: str
    expression: str
    superset_dataset_id: str  # superset dataset to check expression against


async def handle(request: web.Request) -> web.Response:
    return web.Response()
