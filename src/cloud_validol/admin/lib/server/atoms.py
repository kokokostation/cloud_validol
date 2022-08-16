from aiohttp import web
import asyncpg
from typing import Dict


async def get_user_expressions(request: web.Request) -> Dict[str, str]:
    async with request.app['pool'].acquire() as conn:
        rows = await conn.fetch(
            '''
            SELECT 
                name,
                expression
            FROM validol_internal.atom
            WHERE deleted_at IS NULL
        '''
        )

    return {row['name']: row['expression'] for row in rows}


async def insert_user_expression(
    request: web.Request, name: str, expression: str
) -> None:
    try:
        async with request.app['pool'].acquire() as conn:
            await conn.execute(
                '''
                INSERT INTO validol_internal.atom
                    (name, expression)
                VALUES 
                    ($1, $2)
            ''',
                name,
                expression,
            )
    except asyncpg.exceptions.UniqueViolationError:
        raise web.HTTPBadRequest(reason=f'Expression name={name} already exists')


async def delete_user_expression(request: web.Request, name: str) -> None:
    async with request.app['pool'].acquire() as conn:
        await conn.execute(
            '''
            UPDATE validol_internal.atom
            SET deleted_at = NOW()
            WHERE name = $1
        ''',
            name,
        )


async def update_user_expression(
    request: web.Request, name: str, expression: str
) -> None:
    async with request.app['pool'].acquire() as conn:
        await conn.execute(
            '''
            UPDATE validol_internal.atom
            SET expression = $2
            WHERE name = $1
        ''',
            name,
            expression,
        )
