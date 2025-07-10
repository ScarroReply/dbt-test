with doc_first_invoice as (
    select
        id_DimClienti,
        min(id_DimDataDocumento) as min_id_dim_data_documento
    from {{ ref('stg_vw_fact_fatture_cdg') }}
    -- where id_DimDataDocumento = @periodo -- Questo filtro sarà gestito nel modello chiamante
    group by id_DimClienti
),

udf_specialty_data as (
    select * from {{ ref('udf_specialty') }}
),

udf_get_idclientiwn_data as (
    select * from {{ ref('udf_get_idclientiwn') }}
),

final_select as (
    select distinct
        'D' as record_type,
        'IBLKIF' as interface_file_name,
        'I' as mutation_code,
        'IT' as country_code,
        -- Division Code will be handled by the calling model
        con.bk_CustomerNo as client_number_1,
        '' as client_number_2, -- Not explicitly mapped in PDF, assuming empty
        '' as client_number_3, -- Not explicitly mapped in PDF, assuming empty
        left(coalesce(cli.RagioneSociale, ''), 50) as client_name,
        coalesce(cli.Indirizzo, '') as street_name,
        coalesce(cli.cap, '') as postal_code,
        coalesce(cli.Citta, '') as municipality,
        coalesce(cliam.CodInternazionale, '') as concern_code,
        '' as filler, -- Not explicitly mapped in PDF, assuming empty
        max(case when con.ClientStatus = 'PROSPECT' then '1'
                 when con.ClientStatus = 'MAI' then '1'
                 when con.ClientStatus = 'POTENZIALE' then '1'
                 when con.ClientStatus = 'PERSO' then '2'
                 when con.ClientStatus = 'LATENTE' then '3'
                 when con.ClientStatus = 'ATTIVO' then '3'
                 when con.ClientStatus = 'NUOVO' then '3'
                 when con.ClientStatus = 'SVILUPPATO' then '3'
                 else '0' end) as client_status,
        coalesce(dfi.min_id_dim_data_documento, '00000000') as date_first_invoice,
        '' as report_record -- Will be calculated in the final model
    from {{ source('dwh_clean_data', 'DimContatti') }} con -- Assuming DimContatti is in dwh_clean_data
    left outer join {{ source('dwh_clean_data', 'DimClienti') }} cli -- Assuming DimClienti is in dwh_clean_data
        on con.bk_CustomerNo = cli.bk_CodiceContabilie
    left outer join {{ ref('stg_vw_fact_fatture_cdg') }} cdg
        on cdg.id_DimClienti = cli.id_DimClienti
        -- and cdg.id_DimDataDocumento = @periodo -- This filter will be handled in the calling model
    left outer join doc_first_invoice dfi
        on dfi.id_DimClienti = cli.id_DimClienti
    left outer join {{ source('dwh_clean_data', 'DimAlbero') }} alb -- Assuming DimAlbero is in dwh_clean_data
        on cdg.id_DimAlbero = alb.id_DimAlbero
    left outer join {{ source('dwh_clean_data', 'DimContrattiPrestazione') }} cp -- Assuming DimContrattiPrestazione is in dwh_clean_data
        on cdg.id_DimContrattiPrestazione = cp.id_DimContrattiPrestazione
    left outer join {{ source('dwh_clean_data', 'DimContrattiFornitura') }} forn -- Assuming DimContrattiFornitura is in dwh_clean_data
        on cp.cf = forn.bk_ContrattoFornitura
    left outer join {{ ref('stg_tbl_gruppi_clienti') }} cliam
        on con.bk_CustomerNo = cliam.CodInternazionale
    cross apply udf_specialty_data s
    where
        con.ClientStatus is not null
        and con.bk_Unit is not null
        and con.bk_CustomerNo <> '-1'
        -- and not {{ ref('udf_get_idclientiwn') }}(con.bk_CustomerNo) is null -- This function call needs to be handled carefully
    group by
        con.bk_CustomerNo,
        cli.RagioneSociale,
        cli.Indirizzo,
        cli.cap,
        cli.Citta,
        cliam.CodInternazionale,
        dfi.min_id_dim_data_documento
)

select * from final_select
