{{ config(materialized="view") }}

with src as (select * from {{ source("cleaned", "token_event_scopes") }})
select
    timestamp,
    date_trunc('day', timestamp) as event_date,
    date_trunc('hour', timestamp) as event_hour,
    unique_id,
    scope_name,
    scope_family,
    product_bucket,
    service,
    is_readonly
from src
