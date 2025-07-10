with source as (

    select * from {{ source('stg_data', 'tblGruppiClienti') }}

),

renamed as (

    select
        CodInternazionale,
        ConcernName,
        ConcernLevel,
        UpperLevel

    from source

)

select * from renamed
