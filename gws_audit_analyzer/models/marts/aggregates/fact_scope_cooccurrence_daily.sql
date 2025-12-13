{{
    config(
        materialized="incremental",
        unique_key="scope_pair_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    scoped as (
        select event_date, unique_id, user_key, scope_name
        from {{ ref("fact_token_scope") }}
        {% if is_incremental() %}
            where
                event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval 1 day
        {% endif %}
    ),

    pairs as (
        select
            a.event_date,
            a.unique_id,
            a.user_key,
            least(a.scope_name, b.scope_name) as scope_a,
            greatest(a.scope_name, b.scope_name) as scope_b
        from scoped a
        join scoped b on a.unique_id = b.unique_id and a.scope_name < b.scope_name
    ),

    agg as (
        select
            {{ dbt_utils.generate_surrogate_key(["scope_a", "scope_b", "event_date"]) }}
            as scope_pair_day_key,
            event_date,
            scope_a,
            scope_b,
            count(*) as cooccurrence_count,
            count(distinct unique_id) as distinct_events,
            count(distinct user_key) as distinct_users
        from pairs
        group by event_date, scope_a, scope_b
    )

select *
from agg
