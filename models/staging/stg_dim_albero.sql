with source as (

    select * from {{ source('dwh_clean_data', 'DimAlbero') }}

),

renamed as (

    select
        id_DimAlbero,
        Division

    from source

)

select * from renamed
