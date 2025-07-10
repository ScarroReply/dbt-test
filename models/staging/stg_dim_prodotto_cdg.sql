with source as (

    select * from {{ source('dwh_clean_data', 'DimProdottoCDG') }}

),

renamed as (

    select
        IdProdottoCDG

    from source

)

select * from renamed
