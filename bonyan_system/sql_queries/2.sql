-- Identify the top 10 cities generating the most revenue (cost) for all types of events separately and overall,
-- for last week (2025-06-01 – 2025-06-07)

WITH filtered_data AS (
    SELECT
        caller_city,
        event_type,
        cost
    FROM public.tbl_sub_traffic
    WHERE timestamp BETWEEN '2025-06-01' AND '2025-06-07'
),
revenue_by_event AS (
    SELECT
        caller_city,
        event_type,
        SUM(cost) AS revenue
    FROM filtered_data
    GROUP BY caller_city, event_type
),
pivoted AS (
    SELECT
        caller_city,
        COALESCE(SUM(CASE WHEN event_type = 'voice' THEN revenue END), 0) AS voice_revenue,
        COALESCE(SUM(CASE WHEN event_type = 'sms' THEN revenue END), 0) AS sms_revenue,
        COALESCE(SUM(CASE WHEN event_type = 'data' THEN revenue END), 0) AS data_revenue,
        SUM(revenue) AS total_revenue
    FROM revenue_by_event
    GROUP BY caller_city
),
ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_revenue DESC) AS rank
    FROM pivoted
)

SELECT
    caller_city,
    voice_revenue,
    sms_revenue,
    data_revenue,
    total_revenue,
    rank
FROM ranked
WHERE rank <= 10
ORDER BY rank;
