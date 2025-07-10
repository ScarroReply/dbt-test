with source as (

    select * from {{ source('dwh_clean_data', 'DimSocieta') }}

),

renamed as (

    select
        id_DimSocieta,
        bk_Societa

    from source

)

select * from renamed
