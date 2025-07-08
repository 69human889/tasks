-- List the top 10 subscribers (caller_msisdn) by total event cost across all types

WITH ranked_subscribers AS (
    SELECT
        caller_msisdn,
        SUM(cost) AS total_cost,
        RANK() OVER (ORDER BY SUM(cost) DESC) AS rank
    FROM
        public.tbl_sub_traffic
    GROUP BY
        caller_msisdn
)
SELECT
    caller_msisdn,
    total_cost,
    rank
FROM
    ranked_subscribers
WHERE
    rank <= 10
ORDER BY
    rank;
