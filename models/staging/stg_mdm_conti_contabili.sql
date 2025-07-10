with source as (

    select * from {{ source('mds_data', 'mdm.Conti_Contabili') }}

),

renamed as (

    select
        Name,
        Anno

    from source

)

select * from renamed
