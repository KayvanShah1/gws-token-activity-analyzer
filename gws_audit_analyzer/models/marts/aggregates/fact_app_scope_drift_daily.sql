{{
    config(
        materialized="incremental",
        unique_key="app_scope_drift_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

{% set drift_lookback_days = 7 %}

-- Day-over-day scope set drift per app (adds/removes)
with
    -- distinct (app_id, scope_key, event_date)
    app_scope_daily as (
        select te.app_id, ts.scope_key, ts.event_date
        from {{ ref("fact_token_scope") }} ts
        join
            {{ ref("fact_token_event") }} te
            on te.unique_id = ts.unique_id
            and te.event_date = ts.event_date  -- add date guard
        where
            te.app_id is not null and ts.scope_key is not null

            {% if is_incremental() %}
                and ts.event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ drift_lookback_days }} day'
            {% endif %}

        group by te.app_id, ts.scope_key, ts.event_date
    ),

    -- total scopes per app/day
    scope_counts as (
        select app_id, event_date, count(*) as total_scopes
        from app_scope_daily
        group by app_id, event_date
    ),

    prior_counts as (
        select
            app_id,
            event_date + interval 1 day as event_date,
            total_scopes as prior_day_scopes
        from scope_counts
    ),

    -- new scopes today that were not present yesterday
    new_scopes as (
        select c.app_id, c.event_date, count(*) as new_scopes_count
        from app_scope_daily c
        left join
            app_scope_daily p
            on p.app_id = c.app_id
            and p.scope_key = c.scope_key
            and p.event_date = c.event_date - interval 1 day
        where p.scope_key is null
        group by c.app_id, c.event_date
    ),

    -- scopes present yesterday but not today
    removed_scopes as (
        select
            p.app_id,
            p.event_date + interval 1 day as event_date,
            count(*) as removed_scopes_count
        from app_scope_daily p
        left join
            app_scope_daily c
            on c.app_id = p.app_id
            and c.scope_key = p.scope_key
            and c.event_date = p.event_date + interval 1 day
        where c.scope_key is null
        group by p.app_id, p.event_date + interval 1 day
    ),

    combined as (
        select
            coalesce(sc.app_id, ns.app_id, rs.app_id) as app_id,
            coalesce(sc.event_date, ns.event_date, rs.event_date) as event_date,
            coalesce(sc.total_scopes, 0) as total_scopes,
            coalesce(pc.prior_day_scopes, 0) as prior_day_scopes,
            coalesce(ns.new_scopes_count, 0) as new_scopes_count,
            coalesce(rs.removed_scopes_count, 0) as removed_scopes_count
        from scope_counts sc
        full outer join
            new_scopes ns on ns.app_id = sc.app_id and ns.event_date = sc.event_date
        full outer join
            removed_scopes rs
            on rs.app_id = coalesce(sc.app_id, ns.app_id)
            and rs.event_date = coalesce(sc.event_date, ns.event_date)
        left join
            prior_counts pc
            on pc.app_id = coalesce(sc.app_id, ns.app_id, rs.app_id)
            and pc.event_date = coalesce(sc.event_date, ns.event_date, rs.event_date)
    )

select
    {{ dbt_utils.generate_surrogate_key(["app_id", "event_date"]) }}
    as app_scope_drift_day_key,
    app_id,
    event_date,
    total_scopes,
    prior_day_scopes,
    new_scopes_count,
    removed_scopes_count,
    (new_scopes_count - removed_scopes_count) as net_scope_change,
    (new_scopes_count > 0 or removed_scopes_count > 0) as has_scope_drift
from combined
where app_id is not null and event_date is not null
;
