{{
    config(
        materialized="incremental",
        unique_key="app_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

-- Collect all events that have an app dimension
with
    app_events as (

        select app_id, event_date, user_key, unique_id as event_id, 'token' as source
        from {{ ref("fact_token_event") }}
        where app_id is not null

        union all
        select app_id, event_date, user_key, unique_id, 'admin'
        from {{ ref("fact_admin_event") }}
        where app_id is not null

        union all
        select app_id, event_date, user_key, unique_id, 'drive'
        from {{ ref("fact_drive_event") }}
        where app_id is not null
    ),

    -- incremental filter
    filtered as (
        select *
        from app_events

        {% if is_incremental() %}
            where
                event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval 1 day
        {% endif %}
    ),

    agg as (
        select
            {{ dbt_utils.generate_surrogate_key(["app_id", "event_date"]) }}
            as app_day_key,

            app_id,
            event_date,

            count(*) as total_events,
            count(distinct user_key) as distinct_users,
            count(distinct event_id) as distinct_events,

            count_if(source = 'token') as token_events,
            count_if(source = 'admin') as admin_events,
            count_if(source = 'drive') as drive_events
        from filtered
        group by app_id, event_date
    )

select *
from agg
