{{
    config(
        materialized="incremental",
        unique_key="app_baseline_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

-- Daily app baseline (14d/30d) using all-source events + token-only bytes (explicitly
-- labeled)
{% set rebuild_lookback_days = 45 %}

with
    app_token_daily as (
        select app_id, event_date, sum(coalesce(num_bytes, 0)) as token_total_bytes
        from {{ ref("fact_token_event") }}
        where
            app_id is not null

            {% if is_incremental() %}
                and event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ rebuild_lookback_days }} day'
            {% endif %}
        group by app_id, event_date
    ),

    app_daily as (
        select
            {{ dbt_utils.generate_surrogate_key(["a.app_id", "a.event_date"]) }}
            as app_baseline_day_key,
            a.app_id,
            a.event_date,
            a.total_events,  -- all-source events
            a.distinct_users,  -- all-source distinct users
            coalesce(t.token_total_bytes, 0) as token_total_bytes
        from {{ ref("fact_app_activity_daily") }} a
        left join
            app_token_daily t on t.app_id = a.app_id and t.event_date = a.event_date

        {% if is_incremental() %}
            where
                a.event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ rebuild_lookback_days }} day'
        {% endif %}
    ),

    windowed as (
        select
            a.*,

            avg(a.total_events) over (
                partition by a.app_id
                order by a.event_date
                rows between 14 preceding and 1 preceding
            ) as mean_events_14d,
            stddev_pop(a.total_events) over (
                partition by a.app_id
                order by a.event_date
                rows between 14 preceding and 1 preceding
            ) as std_events_14d,

            avg(a.total_events) over (
                partition by a.app_id
                order by a.event_date
                rows between 30 preceding and 1 preceding
            ) as mean_events_30d,
            stddev_pop(a.total_events) over (
                partition by a.app_id
                order by a.event_date
                rows between 30 preceding and 1 preceding
            ) as std_events_30d,

            avg(a.token_total_bytes) over (
                partition by a.app_id
                order by a.event_date
                rows between 14 preceding and 1 preceding
            ) as mean_token_bytes_14d,
            stddev_pop(a.token_total_bytes) over (
                partition by a.app_id
                order by a.event_date
                rows between 14 preceding and 1 preceding
            ) as std_token_bytes_14d,

            avg(a.token_total_bytes) over (
                partition by a.app_id
                order by a.event_date
                rows between 30 preceding and 1 preceding
            ) as mean_token_bytes_30d,
            stddev_pop(a.token_total_bytes) over (
                partition by a.app_id
                order by a.event_date
                rows between 30 preceding and 1 preceding
            ) as std_token_bytes_30d
        from app_daily a
    )

select
    app_baseline_day_key,
    app_id,
    event_date,

    total_events,
    distinct_users,

    token_total_bytes,

    mean_events_14d,
    std_events_14d,
    case
        when std_events_14d is null or std_events_14d = 0
        then null
        else (total_events - mean_events_14d) / std_events_14d
    end as event_z_score_14d,
    case
        when mean_events_14d is null or mean_events_14d = 0
        then null
        else (total_events - mean_events_14d) / mean_events_14d
    end as event_pct_change_14d,

    mean_events_30d,
    std_events_30d,
    case
        when std_events_30d is null or std_events_30d = 0
        then null
        else (total_events - mean_events_30d) / std_events_30d
    end as event_z_score_30d,
    case
        when mean_events_30d is null or mean_events_30d = 0
        then null
        else (total_events - mean_events_30d) / mean_events_30d
    end as event_pct_change_30d,

    mean_token_bytes_14d,
    std_token_bytes_14d,
    case
        when std_token_bytes_14d is null or std_token_bytes_14d = 0
        then null
        else (token_total_bytes - mean_token_bytes_14d) / std_token_bytes_14d
    end as token_bytes_z_score_14d,
    case
        when mean_token_bytes_14d is null or mean_token_bytes_14d = 0
        then null
        else (token_total_bytes - mean_token_bytes_14d) / mean_token_bytes_14d
    end as token_bytes_pct_change_14d,

    mean_token_bytes_30d,
    std_token_bytes_30d,
    case
        when std_token_bytes_30d is null or std_token_bytes_30d = 0
        then null
        else (token_total_bytes - mean_token_bytes_30d) / std_token_bytes_30d
    end as token_bytes_z_score_30d,
    case
        when mean_token_bytes_30d is null or mean_token_bytes_30d = 0
        then null
        else (token_total_bytes - mean_token_bytes_30d) / mean_token_bytes_30d
    end as token_bytes_pct_change_30d
from windowed
;
