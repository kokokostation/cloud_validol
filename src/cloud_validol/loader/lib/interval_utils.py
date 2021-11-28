import datetime as dt
import logging
from typing import Optional
from typing import Tuple

logger = logging.getLogger(__name__)


def get_interval(
    interval_slug: str, last_event_dt: Optional[dt.date], global_from: dt.date
) -> Optional[Tuple[dt.date, dt.date]]:
    to_date = dt.date.today()

    if last_event_dt is None:
        logger.info('No data for %s, downloading from %s', interval_slug, global_from)

        return global_from, to_date

    from_date = last_event_dt + dt.timedelta(days=1)
    if from_date >= to_date:
        logger.info('Data for %s is already up-to-date', interval_slug)

        return None

    logger.info(
        '%s is subject to update, downloading from %s', interval_slug, from_date
    )

    return from_date, to_date
