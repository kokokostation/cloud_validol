from typing import Any
from typing import Dict
from typing import Type
from typing import TypeVar

from aiohttp import web
import marshmallow


T = TypeVar('T', bound=Any)


async def deser_request_body(request: web.Request, klass: Type[T]) -> T:
    request_json = await request.json()

    try:
        request_body = klass.Schema().load(request_json)
    except marshmallow.exceptions.ValidationError as exc:
        raise web.HTTPBadRequest(reason=f'Request schema validation failed: {exc}')

    return request_body


def ser_response_body(response: T) -> Dict:
    return response.Schema().dump(response)
