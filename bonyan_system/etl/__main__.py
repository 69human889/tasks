import asyncio

import aiofiles
from aiocsv import AsyncDictReader

import env,db_operations

db = db_operations.Postgres()

async def pre_processing():
    await db.create_table()


async def main():

    await pre_processing()
    async with aiofiles.open(env.csv_file_path, mode="r", encoding="utf-8") as afp:
        count = 0
        records = []
        async for row in AsyncDictReader(afp):
            # Missing Data: caller_msisdn column cannot be null
            if not row.get('caller_msisdn'):
                continue

            event_type = row.get('event_type')

            # Inconsistent Data: event_type should be from the provided list (sms,voice, data)
            if event_type not in ('sms','voice', 'data'):
                continue
            # Missing Data: In voice records (voice event_type), duration column and
            # in data records (data event_type), volume column cannot be null

            if (event_type=='voice' and not row.get('duration')) \
                or (event_type == 'data' and not row.get('volume')):
                continue


            # Inconsistent Data: caller and callee number column should be number
            if not row.get('caller_msisdn').isdigit() \
                or not (row.get('callee_msisdn') and row.get('callee_msisdn').isdigit()):
                continue

            count += 1
            records.append(row)
            if len(records) >= 10000:
                await db.insert_row(records)
                print(f'{count:,}',f'({len(records)}) records inserted to pg')
                records.clear()

        await db.insert_row(records)
        print(f'{count:,}', f'({len(records)}) records inserted to pg')
        records.clear()



if __name__ == '__main__':
    asyncio.run(main())

