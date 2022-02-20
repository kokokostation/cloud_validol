import os
import json
from typing import Dict
from typing import Optional


SECDIST_PATH = '/etc/cloud_validol/secdist.json'


def get_pg_conn_data(conn_data: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    if conn_data is not None:
        return conn_data
    elif os.path.isfile(SECDIST_PATH):
        with open(SECDIST_PATH) as infile:
            data = json.load(infile)

        return data['postgresql']
    else:
        return os.environ


def get_superset_conn_data() -> Dict[str, str]:
    if not os.path.isfile(SECDIST_PATH):
        raise ValueError(f'No secdist at {SECDIST_PATH}')

    with open(SECDIST_PATH) as infile:
        data = json.load(infile)

    return data['superset']