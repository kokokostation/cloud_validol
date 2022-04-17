import asyncpg

from cloud_validol.lib import secdist


async def get_connection_pool():
    conn_data = secdist.get_pg_conn_data()

    return await asyncpg.create_pool(
        user=conn_data.user,
        password=conn_data.password,
        database=conn_data.dbname,
        host=conn_data.host,
    )
