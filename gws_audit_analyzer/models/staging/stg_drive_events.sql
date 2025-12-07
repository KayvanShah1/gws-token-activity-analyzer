{{ config(materialized="view") }}

with src as (select * from {{ source("processed", "drive_events") }})
select
    timestamp,
    date_trunc('day', timestamp) as event_date,
    date_trunc('hour', timestamp) as event_hour,
    unique_id,
    application_name,
    customer_id,
    actor_email,
    actor_profile_id,
    oauth_client_id,
    oauth_app_name,
    impersonation,
    ip_address,
    asn,
    asn_list,
    region_code,
    subdivision_code,
    event_type,
    event_name,
    resource_ids,
    resource_detail_count,
    user_query,
    parsed_query,
    primary_event,
    billable,
    originating_app_id,
    actor_is_collaborator_account,
    resource_id_count,
    applied_label_count,
    applied_label_titles
from src
