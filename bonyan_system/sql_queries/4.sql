-- List the top 10 Roamer subscribers by data volume usage
WITH roamer_data_usage AS (
    SELECT
        caller_msisdn,
        SUM(volume) AS total_data_volume
    FROM
        public.tbl_sub_traffic
    WHERE
        is_roaming = TRUE       -- adjust if your flag is different, e.g., 'yes'
        AND event_type = 'data'
    GROUP BY
        caller_msisdn
),
ranked_roamers AS (
    SELECT
        caller_msisdn,
        total_data_volume,
        RANK() OVER (ORDER BY total_data_volume DESC) AS rank
    FROM
        roamer_data_usage
)
SELECT
    caller_msisdn,
    total_data_volume,
    rank
FROM
    ranked_roamers
WHERE
    rank <= 10
ORDER BY
    rank;
