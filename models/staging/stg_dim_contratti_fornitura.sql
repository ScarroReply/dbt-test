with source as (

    select * from {{ source('dwh_clean_data', 'DimContrattiFornitura') }}

),

renamed as (

    select
        bk_ContrattoFornitura,
        Datalnizio

    from source

)

select * from renamed
