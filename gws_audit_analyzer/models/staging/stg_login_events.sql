{{ config(materialized="view") }}

with src as (select * from {{ source("cleaned", "login_events") }})
select
    timestamp,
    date_trunc('day', timestamp) as event_date,
    date_trunc('hour', timestamp) as event_hour,
    unique_id,
    application_name,
    customer_id,
    actor_email,
    actor_profile_id,
    ip_address,
    asn,
    asn_list,
    region_code,
    subdivision_code,
    event_type,
    event_name,
    resource_ids,
    resource_detail_count,
    login_type,
    login_challenge_methods,
    is_suspicious
from src
