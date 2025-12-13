{{
    config(
        materialized="incremental",
        unique_key="geo_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    events as (
        -- union facts with ip context
        select ip_key, user_key, event_date, unique_id as event_id, 'token' as source
        from {{ ref("fact_token_event") }}
        where ip_key is not null

        union all
        select ip_key, user_key, event_date, unique_id, 'admin'
        from {{ ref("fact_admin_event") }}
        where ip_key is not null

        union all
        select ip_key, user_key, event_date, unique_id, 'login'
        from {{ ref("fact_login_event") }}
        where ip_key is not null

        union all
        select ip_key, user_key, event_date, unique_id, 'drive'
        from {{ ref("fact_drive_event") }}
        where ip_key is not null

        union all
        select ip_key, user_key, event_date, unique_id, 'saml'
        from {{ ref("fact_saml_event") }}
        where ip_key is not null
    ),

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

    enriched as (
        select
            e.event_date,
            d.region_code,
            d.subdivision_code,
            d.asn,
            e.user_key,
            e.event_id,
            e.source
        from filtered e
        left join {{ ref("dim_ip_address") }} d on d.ip_key = e.ip_key
    ),

    agg as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "coalesce(region_code, 'unk')",
                        "coalesce(subdivision_code, 'unk')",
                        "coalesce(cast(asn as varchar), 'unk')",
                        "event_date",
                    ]
                )
            }} as geo_day_key,

            event_date,
            region_code,
            subdivision_code,
            asn,

            count(*) as total_events,
            count(distinct event_id) as distinct_events,
            count(distinct user_key) as distinct_users,

            count_if(source = 'token') as token_events,
            count_if(source = 'admin') as admin_events,
            count_if(source = 'login') as login_events,
            count_if(source = 'drive') as drive_events,
            count_if(source = 'saml') as saml_events
        from enriched
        group by event_date, region_code, subdivision_code, asn
    )

select *
from agg
