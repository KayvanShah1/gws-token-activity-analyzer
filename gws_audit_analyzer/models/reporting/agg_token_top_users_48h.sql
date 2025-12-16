{{ config(materialized="view") }}

with
    window_bounds as (
        select
            max(timestamp) as max_ts,
            max(timestamp) - interval '48 hours' as window_start
        from {{ ref("fact_token_event") }}
    ),

    filtered as (
        select b.timestamp, b.actor_email, b.actor_profile_id, b.num_bytes
        from {{ ref("fact_token_event") }} b
        cross join window_bounds w
        where b.timestamp >= w.window_start
    ),

    agg as (
        select
            actor_email,
            actor_profile_id,
            count(*) as event_count,
            sum(num_bytes) as total_bytes
        from filtered
        group by actor_email, actor_profile_id
    )

select
    actor_email,
    actor_profile_id,
    event_count,
    total_bytes,
    (select window_start from window_bounds) as window_start,
    (select max_ts from window_bounds) as window_end,
    row_number() over (order by event_count desc, total_bytes desc) as rank_by_events
from agg
order by rank_by_events
