{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    base as (

        select *
        from {{ ref("stg_token_events") }}

        {% if is_incremental() %}

            where
                timestamp >= (
                    select coalesce(max(timestamp), timestamp '1970-01-01')
                    from {{ this }}
                )
                - interval 5 minute

        {% endif %}
    ),

    joined as (
        select
            -- event grain / time
            b.unique_id,
            b.timestamp,
            b.event_date,
            b.event_hour,
            extract(hour from b.timestamp) as hour_of_day,

            -- foreign keys
            u.user_key,
            i.ip_key,
            -- if you decide to add a time_key:
            -- t.date_day AS time_key,
            -- natural keys kept for debugging
            b.actor_profile_id,
            b.actor_email,
            b.ip_address,

            -- application
            b.client_id as app_id,
            a.app_name,
            a.client_type,
            a.api_name,
            b.app_name,
            b.customer_id,

            -- geo
            b.region_code,
            b.subdivision_code,

            -- measures
            b.num_bytes,
            b.scope_count,
            b.resource_detail_count,

            -- flags
            b.has_drive_scope,
            b.has_gmail_scope,
            b.has_admin_scope,

            -- degenerate dimensions
            b.event_type,
            b.event_name,
            b.method_name,
            b.product_buckets
        from base b
        left join
            {{ ref("dim_user") }} u
            on u.actor_profile_id = b.actor_profile_id
            and u.actor_email = b.actor_email

        left join {{ ref("dim_ip_address") }} i on i.ip_address = b.ip_address

        left join {{ ref("dim_application") }} a on a.app_id = b.client_id

    -- if you want a time_key:
    -- LEFT JOIN {{ ref('dim_time') }} t
    -- ON t.date_day = b.event_date
    )

select *
from joined
