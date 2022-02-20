import dataclasses
import functools
import json


def dataclass2json(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        return json.dumps(dataclasses.asdict(func(*args, **kwargs)))

    return wrapped


@dataclass2json
@dataclasses.dataclass
class Error:
    code: str
    message: str
