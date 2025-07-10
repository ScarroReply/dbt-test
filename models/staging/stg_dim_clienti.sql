with source as (

    select * from {{ source('dwh_clean_data', 'DimClienti') }}

),

renamed as (

    select
        bk_CodiceContabilie

    from source

)

select * from renamed
