with source as (

    select * from {{ source('olap_data', 'vwFactCostoTotaleperCP') }}

),

renamed as (

    select
        id_DimSocieta,
        QuantitaCosto,
        CostoUnitario,
        CostoTotale,
        id_DimDataDocumento,
        id_DimDataCompetenza,
        id_DimAlbero,
        id_DimClienti,
        id_DimContrattiPrestazione,
        id_DimLavoratori,
        id_DimCausali,
        id_DimContatti,
        id_DimContrattiQuadro,
        id_DimProdottoCDG,
        id_dim_Transfer,
        id_DimContrattiFornitura,
        Id_DimSorgente,
        Id_DimTipoBusiness

    from source

)

select * from renamed
