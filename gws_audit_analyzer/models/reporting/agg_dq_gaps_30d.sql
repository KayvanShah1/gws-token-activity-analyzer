{{ config(materialized="view") }}

select *
from {{ ref("dq_app_event_gaps") }}
where event_date >= current_date - interval '30 day'
order by event_date desc, app_id
