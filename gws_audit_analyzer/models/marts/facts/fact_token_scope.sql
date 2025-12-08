{{
    config(
        materialized="incremental",
        unique_key="token_scope_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    scopes as (
        select *
        from {{ ref("stg_token_event_scopes") }}

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
            {{ dbt_utils.generate_surrogate_key(["s.unique_id", "s.scope_name"]) }}
            as token_scope_key,
            s.unique_id,
            s.timestamp,
            s.event_date,
            s.event_hour,
            extract(hour from s.timestamp) as hour_of_day,

            u.user_key,
            i.ip_key,
            d.scope_key,

            s.scope_name,
            s.scope_family,
            s.product_bucket,
            s.service,
            s.is_readonly
        from scopes s
        left join {{ ref("stg_token_events") }} e on e.unique_id = s.unique_id
        left join
            {{ ref("dim_user") }} u
            on u.actor_profile_id = e.actor_profile_id
            and u.actor_email = e.actor_email
        left join {{ ref("dim_ip_address") }} i on i.ip_address = e.ip_address
        left join {{ ref("dim_scope") }} d on d.scope_name = s.scope_name
    )

select *
from joined
