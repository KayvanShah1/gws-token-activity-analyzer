{{ config(materialized="table") }}

with
    bounds as (
        select
            date_trunc('day', min(timestamp)) as min_date,
            date_trunc('day', max(timestamp)) as max_date
        from
            (
                select timestamp
                from {{ ref("stg_token_events") }}
                union all
                select timestamp
                from {{ ref("stg_admin_events") }}
                union all
                select timestamp
                from {{ ref("stg_login_events") }}
                union all
                select timestamp
                from {{ ref("stg_drive_events") }}
                union all
                select timestamp
                from {{ ref("stg_saml_events") }}
            )
    ),
    calendar as (
        -- DuckDB generate_series over dates
        select d::date as date_day
        from bounds, generate_series(min_date, max_date, interval 1 day) as t(d)
    )
select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(doy from date_day) as day_of_year,
    extract(week from date_day) as week_of_year,
    extract(dow from date_day) as day_of_week,
    strftime(date_day, '%A') as day_name,
    strftime(date_day, '%B') as month_name,
    case
        when extract(dow from date_day) in (6, 0) then true else false
    end as is_weekend,
    date_trunc('week', date_day) as week_start,
    date_trunc('month', date_day) as month_start,
    date_trunc('quarter', date_day) as quarter_start,
    case
        when date_day = date_trunc('month', date_day) then true else false
    end as is_month_start,
    case
        when
            date_day
            = (date_trunc('month', date_day) + interval 1 month - interval 1 day)
        then true
        else false
    end as is_month_end
from calendar

order by date_day
