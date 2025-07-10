with source as (

    select * from {{ source('mds_data', 'Mansioni_RASCO_ISCO_SPEC') }}

),

renamed as (

    select
        code_6_dgt_rasco,
        specialty,
        DESC_SPECIALTY,
        DESC_ESTESA_SPECIALTY

    from source

)

select * from renamed
