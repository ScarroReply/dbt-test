with source as (

    select * from {{ source('stg_data', 'vwClientiTransferldDim') }}

),

renamed as (

    select
        id_DimAlbero,
        id_DimClienti,
        id_DimDataInizio,
        id_DimDataFine

    from source

)

select * from renamed
