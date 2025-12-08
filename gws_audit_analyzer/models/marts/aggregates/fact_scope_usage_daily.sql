{{
    config(
        materialized="incremental",
        unique_key="scope_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

-- 1. Pull fact_token_scope (1 row per event-scope pair)
with
    scopes as (
        select scope_key, event_date, unique_id as event_id, user_key, ip_key
        from {{ ref("fact_token_scope") }}
    ),

    -- 2. Incremental filter
    filtered as (
        select *
        from scopes
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
            {{ dbt_utils.generate_surrogate_key(["scope_key", "event_date"]) }}
            as scope_day_key,

            scope_key,
            event_date,

            count(*) as total_scope_usages,
            count(distinct event_id) as distinct_events,
            count(distinct user_key) as distinct_users,
            count(distinct ip_key) as distinct_ips
        from filtered
        group by scope_key, event_date
    )

select *
from agg
