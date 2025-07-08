import asyncio

import aiofiles
from aiocsv import AsyncDictReader

import env,db_operations

db = db_operations.Postgres()

async def pre_processing():
    await db.connect()
    await db.create_table()
    await db.close()


async def main():

    await pre_processing()
    await db.connect()
    async with aiofiles.open(env.csv_file_path, mode="r", encoding="utf-8") as afp:
        async for row in AsyncDictReader(afp):

            print(row)
            break
            # await db.insert_row(row.values())
    await db.close()

if __name__ == '__main__':
    asyncio.run(main())

