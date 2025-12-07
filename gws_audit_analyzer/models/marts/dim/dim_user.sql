{{ config(materialized="table") }}

with
    union_users as (
        select
            actor_profile_id,
            actor_email,
            null as orgunit_path,
            actor_is_collaborator_account
        from {{ ref("stg_drive_events") }}
        union
        select actor_profile_id, actor_email, null, null
        from {{ ref("stg_token_events") }}
        union
        select actor_profile_id, actor_email, null, null
        from {{ ref("stg_admin_events") }}
        union
        select actor_profile_id, actor_email, orgunit_path, null
        from {{ ref("stg_saml_events") }}
        union
        select actor_profile_id, actor_email, null, null
        from {{ ref("stg_login_events") }}
    )
select
    {{ dbt_utils.generate_surrogate_key(["actor_profile_id", "actor_email"]) }}
    as user_key,
    actor_profile_id,
    actor_email,
    max(orgunit_path) as orgunit_path,
    bool_or(coalesce(actor_is_collaborator_account, false)) as is_collaborator
from union_users
group by actor_profile_id, actor_email
