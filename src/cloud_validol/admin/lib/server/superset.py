from aiohttp import web
from typing import List

from cloud_validol.admin.lib import superset
from cloud_validol.admin.lib.server import base as server_base


def parse_dataset_columns(
    dataset: superset.DatasetItemView, user_atoms: List[str]
) -> superset.DatasetColumnsInfo:
    try:
        return superset.parse_dataset_columns(dataset, user_atoms)
    except superset.BasicAtomCollision as exc:
        raise web.HTTPInternalServerError(
            body=server_base.Error(
                code='ATOM_ERROR',
                message=(
                    f'Dataset {dataset.table_name} column name '
                    f'"{exc.column_name}" collides with user defined atom name'
                ),
            )
        )
