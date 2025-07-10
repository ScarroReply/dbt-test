with source as (

    select * from {{ source('dwh_clean_data', 'Bridge_EasyFlex') }}

),

renamed as (

    select
        scd2_valid_from,
        scd2_valid_to

    from source

)

select * from renamed
