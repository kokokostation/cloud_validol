from aiohttp import web
from typing import Any
from typing import Dict
from typing import Type
from typing import TypeVar


T = TypeVar('T', bound=Any)


async def deser_request_body(request: web.Request, klass: Type[T]) -> T:
    request_json = await request.json()
    request_body = klass.Schema().load(request_json)

    return request_body


def ser_response_body(response: T) -> Dict:
    return response.Schema().dump(response)
