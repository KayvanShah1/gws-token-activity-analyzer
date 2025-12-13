{{ config(materialized="view") }}

select *
from {{ ref("dq_duplicate_events_daily") }}
where event_date >= current_date - interval '30 day'
order by event_date desc, source
