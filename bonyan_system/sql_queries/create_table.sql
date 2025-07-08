CREATE TYPE type_eventType AS ENUM ('voice', 'data', 'sms');
CREATE TABLE IF NOT EXISTS tbl_sub_traffic (
"timestamp" timestamp without time zone,
caller_msisdn bigint NOT NULL,
callee_msisdn bigint,
event_type type_eventType,
caller_city nvarchar(64),
callee_city nvarchar(64),
duration real,
volume real,
cost int,
is_roaming boolean
);
