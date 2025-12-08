{{
    config(
        materialized="incremental",
        unique_key="ip_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

-- 1. Collect all events that have an IP dimension
with
    events as (

        select
            ip_key,
            event_date,
            user_key,
            app_id,
            unique_id as event_id,
            'token' as source
        from {{ ref("fact_token_event") }}
        where ip_key is not null

        union all
        select ip_key, event_date, user_key, app_id, unique_id, 'admin'
        from {{ ref("fact_admin_event") }}
        where ip_key is not null

        union all
        select ip_key, event_date, user_key, null as app_id, unique_id, 'login'
        from {{ ref("fact_login_event") }}
        where ip_key is not null

        union all
        select ip_key, event_date, user_key, app_id, unique_id, 'drive'
        from {{ ref("fact_drive_event") }}
        where ip_key is not null

        union all
        select ip_key, event_date, user_key, null as app_id, unique_id, 'saml'
        from {{ ref("fact_saml_event") }}
        where ip_key is not null
    ),

    -- 2. Incremental window filter
    filtered as (
        select *
        from events
        {% if is_incremental() %}
            where
                event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval 1 day
        {% endif %}
    ),

    -- 3. Aggregate metrics
    agg as (
        select
            {{ dbt_utils.generate_surrogate_key(["ip_key", "event_date"]) }}
            as ip_day_key,

            ip_key,
            event_date,

            count(*) as total_events,
            count(distinct user_key) as distinct_users,
            count(distinct app_id) as distinct_apps,

            count_if(source = 'token') as token_events,
            count_if(source = 'admin') as admin_events,
            count_if(source = 'login') as login_events,
            count_if(source = 'drive') as drive_events,
            count_if(source = 'saml') as saml_events

        from filtered
        group by ip_key, event_date
    )

select *
from agg
