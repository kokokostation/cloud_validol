import os

import sqlalchemy


def get_engine():
    user = os.environ['DATABASE_USER']
    password = os.environ['DATABASE_PASSWORD']
    dbname = os.environ['DATABASE_DB']
    dbhost = os.environ['DATABASE_HOST']

    return sqlalchemy.create_engine(f'postgresql+psycopg2://{user}:{password}@{dbhost}/{dbname}')
