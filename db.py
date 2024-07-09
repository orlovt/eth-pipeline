import asyncpg
import asyncio

CONNECTION = "postgres://tsdbadmin:qb2wvgexfzqzlneo@o4e65tiybv.n6wrdgoi1v.tsdb.cloud.timescale.com:37034/tsdb?sslmode=require"


async def main():
    conn = await asyncpg.connect(CONNECTION)
    extensions = await conn.fetch("select extname, extversion from pg_extension")
    for extension in extensions:
        print(extension)
    await conn.close()

asyncio.run(main())