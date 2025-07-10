with source as (

    select * from {{ source('dwh_clean_data', 'DimContrattiPrestazione') }}

),

renamed as (

    select
        id_DimContrattiPrestazione,
        cf,
        bk_ContrattoPrestazione,
        MansioneCodice

    from source

)

select * from renamed
