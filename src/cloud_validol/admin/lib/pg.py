import asyncpg

from cloud_validol.lib import secdist


async def get_connection_pool():
    conn_data = secdist.get_pg_conn_data()

    return await asyncpg.create_pool(
        user=conn_data['DATABASE_USER'],
        password=conn_data['DATABASE_PASSWORD'],
        database=conn_data['DATABASE_DB'],
        host=conn_data['DATABASE_HOST'],
    )
