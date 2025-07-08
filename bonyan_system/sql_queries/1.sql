-- Determine the top 10 cities with the highest total voice call durations.
with cities_cte as (
    select caller_city as city ,duration
    FROM public.tbl_sub_traffic
    where event_type = 'voice'
    union all
    select callee_city,duration
    FROM public.tbl_sub_traffic
    where event_type = 'voice'
),
cities_sum_duration as (
    SELECT city , sum(duration) as total_duration
    FROM cities_cte
    group by city
),
cities_ranked as (
    select city,rank() over(order by total_duration desc) as "rank"
    from cities_sum_duration
)

select city
from cities_ranked
where "rank" <= 10;
order by rank;