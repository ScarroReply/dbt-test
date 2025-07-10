with source as (

    select * from {{ source('mds_data', 'Mansioni_Trascodifica_RND') }}

),

renamed as (

    select
        cod_mansione_randstad_italy,
        desc_mansione_randstad_italy

    from source

)

select * from renamed
