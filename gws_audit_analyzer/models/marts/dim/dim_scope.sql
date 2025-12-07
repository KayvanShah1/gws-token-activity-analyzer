{{ config(materialized="table") }}

select
    {{ dbt_utils.generate_surrogate_key(["scope_name"]) }} as scope_key,
    scope_name,
    max(scope_family) as scope_family,
    max(product_bucket) as product_bucket,
    max(service) as service,
    bool_or(is_readonly) as is_readonly
from {{ ref("stg_token_event_scopes") }}
group by scope_name
