with source as (

    select * from {{ source('stg_data', 'tblMansioni') }}

),

renamed as (

    select
        C_MANIST_COD,
        C_MANIST_DES,
        C_RAGGRMANS_COD,
        C_ISTAT_COD

    from source

)

select * from renamed
