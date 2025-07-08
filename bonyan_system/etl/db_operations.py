import asyncpg

import env

class Postgres:
    def __init__(self):
        self.host = env.postgres_host
        self.user = env.postgres_username
        self.password = env.postgres_password
        self.database = env.postgres_db
        self.connection = asyncpg.Connection

    async def connect(self):
        self.connection:asyncpg.Connection= await asyncpg.connect(host=self.host,user=self.user,password=self.password,database=self.database)

    async def create_table(self):
        async with self.connection.transaction():
            try:
                await self.connection.execute("""
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


    async def insert_row(self,record):
        async with self.connection.transaction():
            await self.connection.execute(
                "INSERT INTO tbl_sub_traffic (timestamp,caller_msisdn,callee_msisdn,event_type,caller_city,callee_city,duration,volume,cost,is_roaming) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                *record
            )
    async def close(self):
        await self.connection.close()
