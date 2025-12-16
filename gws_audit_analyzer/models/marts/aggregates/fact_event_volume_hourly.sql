{{
    config(
        materialized="incremental",
        unique_key="app_hour_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    events as (

        select event_hour, event_date, 'token' as source, unique_id
        from {{ ref("fact_token_event") }}

        union all
        select event_hour, event_date, 'admin', unique_id
        from {{ ref("fact_admin_event") }}

        union all
        select event_hour, event_date, 'login', unique_id
        from {{ ref("fact_login_event") }}

        union all
        select event_hour, event_date, 'drive', unique_id
        from {{ ref("fact_drive_event") }}

        union all
        select event_hour, event_date, 'saml', unique_id
        from {{ ref("fact_saml_event") }}
    ),

    filtered as (
        select *
        from events
        {% if is_incremental() %}
            where
                event_hour >= (
                    select coalesce(max(event_hour), timestamp '1970-01-01')
                    from {{ this }}
                )
                - interval 1 hour
        {% endif %}
    ),

    agg as (
        select
            {{ dbt_utils.generate_surrogate_key(["source", "event_hour"]) }}
            as app_hour_key,
            source,
            event_hour,
            event_date,
            count(*) as total_events,
            count(distinct unique_id) as distinct_events
        from filtered
        group by source, event_hour, event_date
    )

select *
from agg
