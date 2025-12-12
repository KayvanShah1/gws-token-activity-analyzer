{{ config(materialized="view") }}

with src as (select * from {{ source("processed", "token_events") }})
select
    "timestamp",
    date_trunc('day', timestamp) as event_date,
    date_trunc('hour', timestamp) as event_hour,
    unique_id,
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
    method_name,
    num_bytes,
    api_name,
    client_id,
    accessing_app_name,
    client_type,
    scope_count,
    product_buckets,
    has_drive_scope,
    has_gmail_scope,
    has_admin_scope
from src
