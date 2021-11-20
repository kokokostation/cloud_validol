import os
import json

import sqlalchemy


SECDIST_PATH = '/etc/cloud_validol/secdist.json'


def get_connstr() -> str:
    if os.path.isfile(SECDIST_PATH):
        with open(SECDIST_PATH) as infile:
            data = json.load(infile)

        conn_data = data['postgresql']
    else:
        conn_data = os.environ

    user = conn_data['DATABASE_USER']
    password = conn_data['DATABASE_PASSWORD']
    dbname = conn_data['DATABASE_DB']
    dbhost = conn_data['DATABASE_HOST']

    return f'postgresql+psycopg2://{user}:{password}@{dbhost}/{dbname}'


def get_engine():
    return sqlalchemy.create_engine(get_connstr())
