{{
    config(
        materialized="incremental",
        unique_key="user_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

-- Union all facts with a "source" label
with
    all_events as (

        select user_key, event_date, 'token' as source, unique_id as event_id
        from {{ ref("fact_token_event") }}

        union all
        select user_key, event_date, 'admin', unique_id
        from {{ ref("fact_admin_event") }}

        union all
        select user_key, event_date, 'login', unique_id
        from {{ ref("fact_login_event") }}

        union all
        select user_key, event_date, 'drive', unique_id
        from {{ ref("fact_drive_event") }}

        union all
        select user_key, event_date, 'saml', unique_id
        from {{ ref("fact_saml_event") }}

    ),

    -- Incremental filter
    filtered as (
        select *
        from all_events

        {% if is_incremental() %}
            where
                event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval 1 day  -- 1-day overlap
        {% endif %}
    ),

    agg as (
        select
            {{ dbt_utils.generate_surrogate_key(["user_key", "event_date"]) }}
            as user_day_key,

            user_key,
            event_date,

            count(*) as total_events,

            count_if(source = 'token') as token_events,
            count_if(source = 'admin') as admin_events,
            count_if(source = 'login') as login_events,
            count_if(source = 'drive') as drive_events,
            count_if(source = 'saml') as saml_events,

            count(distinct event_id) as distinct_events
        from filtered
        group by user_key, event_date
    )

select *
from agg
