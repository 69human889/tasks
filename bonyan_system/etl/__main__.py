import asyncio

import aiofiles
from aiocsv import AsyncDictReader
from copy import deepcopy
from datetime import datetime

import env,db_operations

db = db_operations.Postgres()

async def pre_processing():
    await db.create_table()

async def record_schema(record:dict)->dict:
    record['timestamp'] = datetime.strptime(record['timestamp'], "%Y-%m-%d %H:%M:%S")
    record['caller_msisdn'] = int(record['caller_msisdn'])
    record['callee_msisdn'] = int(record['callee_msisdn']) if record.get('callee_msisdn') else None
    record['duration'] = float(record['duration'])
    record['volume'] = float(record['volume'])
    record['cost'] = float(record['cost'])
    record['is_roaming'] = bool(int(record['is_roaming']))
    return record

async def main(batch_size:int):
    await pre_processing()
    async with aiofiles.open(env.csv_file_path, mode="r", encoding="utf-8") as afp:
        count = 0
        records = []
        async for record in AsyncDictReader(afp):
            # cleaning data.
            ## Missing Data: caller_msisdn column cannot be null
            if not record.get('caller_msisdn'):
                continue

            event_type = record.get('event_type')

            ## Inconsistent Data: event_type should be from the provided list (sms,voice, data)
            if event_type not in ('sms','voice', 'data'):
                continue
            ## Missing Data: In voice records (voice event_type), duration column and
            ## in data records (data event_type), volume column cannot be null

            if (event_type=='voice' and not record.get('duration')) \
                or (event_type == 'data' and not record.get('volume')):
                continue


            ## Inconsistent Data: caller and callee number column should be number
            if not record.get('caller_msisdn').isdigit() \
                or not (record.get('callee_msisdn') and record.get('callee_msisdn').isdigit()):
                continue

            # specify schema for record.
            record = await record_schema(deepcopy(record))

            count += 1
            # group records
            records.append(record)
            # insert base on batch size
            if len(records) >= batch_size:
                await db.insert_row(records)
                print(f'{count:,}',f'({len(records)}) records inserted to pg')
                records.clear()
        # insert remaining records.
        await db.insert_row(records)
        print(f'{count:,}', f'({len(records)}) records inserted to pg')
        records.clear()



if __name__ == '__main__':
    try:
        asyncio.run(main(10_000))
    except KeyboardInterrupt:
        print('Exit.')

