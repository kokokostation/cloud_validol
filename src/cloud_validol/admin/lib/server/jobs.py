import dataclasses
import enum
import functools
from typing import Coroutine
from typing import Dict
from typing import Optional
import uuid

import aiojobs
from aiojobs import aiohttp as aiojobs_aiohttp
from aiohttp import web


class JobStatus(enum.Enum):
    SUCCESS = 'SUCCESS'
    IN_PROGRESS = 'IN_PROGRESS'
    FAILURE = 'FAILURE'


_TERMINAL_STATUSES = [JobStatus.SUCCESS, JobStatus.FAILURE]


@dataclasses.dataclass
class JobInfo:
    status: JobStatus
    message: Optional[str] = None


def exception_handler(
    jobs: Dict[str, JobInfo], scheduler: aiojobs.Scheduler, context: Dict
) -> None:
    job = context['job']
    exc = context['exception']
    job_id = job.validol_id

    jobs[job_id] = JobInfo(status=JobStatus.FAILURE, message=str(exc))


def setup(app: web.Application) -> None:
    app['jobs'] = {}
    aiojobs_aiohttp.setup(
        app, exception_handler=functools.partial(exception_handler, app['jobs'])
    )


async def spawn(request: web.Request, coro: Coroutine) -> str:
    job_id = uuid.uuid4().hex

    job = await aiojobs_aiohttp.spawn(request, coro)
    job.validol_id = job_id

    request.app['jobs'][job_id] = JobInfo(status=JobStatus.IN_PROGRESS)

    return job_id


def get_job_info(request: web.Request, job_id: str) -> Optional[JobInfo]:
    job_info = request.app['jobs'].get(job_id)
    if job_info is None:
        return None

    if job_info.status in _TERMINAL_STATUSES:
        return job_info

    # checking if job is out of scheduler. if it is, then it has succeeded
    scheduler = aiojobs_aiohttp.get_scheduler(request)
    curr_job_ids = [job.validol_id for job in scheduler]
    if job_id not in curr_job_ids:
        job_info = JobInfo(status=JobStatus.SUCCESS)
        request.app['jobs'][job_id] = job_info

    return job_info
