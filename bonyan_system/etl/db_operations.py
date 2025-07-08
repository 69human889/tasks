from email.quoprimime import body_length

import asyncpg
from datetime import datetime

import env

class Postgres:
    def __init__(self):
        self.host = env.postgres_host
        self.user = env.postgres_username
        self.password = env.postgres_password
        self.database = env.postgres_db

    async def connect(self)->asyncpg.Connection:
        return await asyncpg.connect(host=self.host,user=self.user,password=self.password,database=self.database)

    async def create_table(self):
        connection = await self.connect()
        async with connection.transaction():
            try:
                await connection.execute("""
                CREATE TYPE type_eventType AS ENUM ('voice', 'data', 'sms');
                CREATE TABLE IF NOT EXISTS tbl_sub_traffic (
                "timestamp" timestamp without time zone,
                caller_msisdn bigint NOT NULL,
                callee_msisdn bigint,
                event_type type_eventType,
                caller_city varchar(64),
                callee_city varchar(64),
                duration real,
                volume real,
                cost int,
                is_roaming boolean
                );""")
            except asyncpg.exceptions.DuplicateObjectError:
                pass
        await connection.close()


    async def insert_row(self,records:list[dict]):
        for record in records:
            record['timestamp'] = datetime.strptime(record['timestamp'], "%Y-%m-%d %H:%M:%S")
            record['caller_msisdn'] = int(record['caller_msisdn'])
            record['callee_msisdn'] = int(record['callee_msisdn']) if record.get('callee_msisdn') else None
            record['duration'] = float(record['duration'])
            record['volume'] = float(record['volume'])
            record['cost'] = float(record['cost'])
            record['is_roaming'] = bool(int(record['is_roaming']))

        connection = await self.connect()
        async with connection.transaction():

            try:
                await connection.executemany(
                    "INSERT INTO tbl_sub_traffic (timestamp,caller_msisdn,callee_msisdn,event_type,caller_city,callee_city,duration,volume,cost,is_roaming) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                    [tuple(record.values()) for record in records]
                )
            except asyncpg.exceptions.DataError as e:
                raise asyncpg.exceptions.DataError(*e.args)
        await connection.close()

