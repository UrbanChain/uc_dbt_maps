-- models/staging/lkup_accred_org_names.sql

with source as (
    select *
    from {{ source('lkup_accred_org_names_source','lkup_accred_org_names') }}
)

select
        "OrganisationReference" AS generator_id,
        "OrganisationName" AS fit_entity
From source