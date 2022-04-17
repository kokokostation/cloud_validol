import dataclasses
import os
import json


SECDIST_PATH = '/etc/cloud_validol/secdist.json'


@dataclasses.dataclass
class PgConnData:
    user: str
    password: str
    dbname: str
    host: str


@dataclasses.dataclass
class SupersetConnData:
    user: str
    password: str
    base_url: str


def get_pg_conn_data() -> PgConnData:
    if os.path.isfile(SECDIST_PATH):
        with open(SECDIST_PATH) as infile:
            data = json.load(infile)

        conn_data = data['postgresql']
    else:
        conn_data = os.environ

    return PgConnData(
        user=conn_data['DATABASE_USER'],
        password=conn_data['DATABASE_PASSWORD'],
        dbname=conn_data['DATABASE_DB'],
        host=conn_data['DATABASE_HOST'],
    )


def get_superset_conn_data() -> SupersetConnData:
    if not os.path.isfile(SECDIST_PATH):
        raise ValueError(f'No secdist at {SECDIST_PATH}')

    with open(SECDIST_PATH) as infile:
        data = json.load(infile)

    conn_data = data['superset']

    return SupersetConnData(
        user=conn_data['USER'],
        password=conn_data['PASSWORD'],
        base_url=conn_data['BASE_URL'],
    )
