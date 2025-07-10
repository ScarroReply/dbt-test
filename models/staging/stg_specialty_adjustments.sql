with source as (

    select * from {{ source('iam_data', 'SPECIALTY_ADJUSTMENTS') }}

),

renamed as (

    select
        Cod_Region,
        Region,
        Cod_Specialty,
        Specialty,
        Cod_Specialty_Adj,
        Specialty_Adj

    from source

)

select * from renamed
