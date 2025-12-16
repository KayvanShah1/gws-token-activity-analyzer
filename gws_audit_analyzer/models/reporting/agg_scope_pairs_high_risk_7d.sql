{{ config(materialized="view") }}

with
    pairs as (
        select
            c.event_date,
            c.scope_a,
            c.scope_b,
            c.cooccurrence_count,
            c.distinct_events,
            c.distinct_users,
            da.service as service_a,
            db.service as service_b
        from {{ ref("fact_scope_cooccurrence_daily") }} c
        left join {{ ref("dim_scope") }} da on da.scope_name = c.scope_a
        left join {{ ref("dim_scope") }} db on db.scope_name = c.scope_b
        where c.event_date >= current_date - interval '7 day'
    )

select
    event_date,
    scope_a,
    scope_b,
    service_a,
    service_b,
    cooccurrence_count,
    distinct_events,
    distinct_users
from pairs
where
    service_a in ('admin', 'drive', 'gmail') or service_b in ('admin', 'drive', 'gmail')
order by cooccurrence_count desc
