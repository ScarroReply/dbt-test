with source as (

    select * from {{ source('dwh_clean_data', 'FactFatture') }}

),

renamed as (

    select
        id_fact,
        id_DimDataDocumento,
        id_DimDataCompetenza,
        id_DimClienti,
        id_DimContiContabili,
        id_DimContrattiPrestazione,
        id_DimDocumenti,
        id_DimLineeBusiness,
        id_DimStatiElaborazione,
        id_DimCausali,
        id_DimLavoratori,
        id_DimAlbero,
        id_DimContatti,
        id_DimBusinessLineCDG,
        id_dimProdottoCDG,
        Quantita as quantita,
        PrezzoUnitario as prezzo_unitario,
        Fatturato as fatturato,
        CostoTecnicoUnitario as costo_tecnico_unitario,
        CostoPresuntoUnitario as costo_presunto_unitario

    from source

)

select * from renamed
