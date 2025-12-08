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
        from {{ ref("stg_drive_events") }}

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
            b.unique_id,
            b.timestamp,
            b.event_date,
            b.event_hour,
            extract(hour from b.timestamp) as hour_of_day,

            u.user_key,
            i.ip_key,

            b.actor_profile_id,
            b.actor_email,
            b.ip_address,
            b.customer_id,

            -- application (if dim_application includes drive oauth_client_id)
            b.oauth_client_id as app_id,
            a.app_name,
            a.client_type,
            a.api_name,

            b.region_code,
            b.subdivision_code,

            -- measures
            b.resource_detail_count,
            b.resource_id_count,
            b.applied_label_count,

            -- flags
            b.primary_event,
            b.billable,
            b.actor_is_collaborator_account,

            -- attributes
            b.user_query,
            b.parsed_query,
            b.originating_app_id,
            b.applied_label_titles,
            b.event_type,
            b.event_name
        from base b
        left join
            {{ ref("dim_user") }} u
            on u.actor_profile_id = b.actor_profile_id
            and u.actor_email = b.actor_email
        left join {{ ref("dim_ip_address") }} i on i.ip_address = b.ip_address
        left join {{ ref("dim_application") }} a on a.app_id = b.oauth_client_id
    )

select *
from joined
