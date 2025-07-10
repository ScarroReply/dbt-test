with source as (

    select * from {{ source('dwh_clean_data', 'DimCausali') }}

),

renamed as (

    select
        id_DimCausali,
        bk_GruppoStatistico,
        bk_Causale_WN

    from source

)

select * from renamed
