with cteFT as (
    select
        substring(cast(ft.id_DimDataDocumento as string), 1, 4) as accounting_year,
        substring(cast(ft.id_DimDataDocumento as string), 5, 2) as accounting_period,
        con.CodiceLavoratore as codicelavoratore,
        cli.bk_CodiceContabilie as nr_cliente,
        cast(alb.bk_Unit as string) as client_number_2,
        con.bk_CP as nr_contratto_prestazione,
        ft.Fatturato as importo_totale_fp,
        alb.Division || '_' || coalesce(spe_adj.DESC_SPECIALTY, spe.DESC_SPECIALTY) || '_TEMP' as division_code,
        coalesce(caus.bk_GruppoStatistico, -1) as new_statistic_group,
        coalesce(con.MansioneCodice, '') as position,
        replace(coalesce(cast(con.Datalnizio as string), '19000101'), '-', '') as start_date_job_order,
        forn.TariffaOraOrdinaria as client_rate
    from {{ ref('stg_vw_fact_fatture_cdg') }} ft
    inner join {{ ref('stg_dim_prodotto_cdg') }} pr
        on ft.id_DimProdottoCDG = pr.IdProdottoCDG
    inner join {{ ref('stg_dim_societa') }} s
        on ft.id_DimSocieta = s.Id_DimSocieta
    left outer join {{ ref('stg_dim_causali') }} caus
        on ft.id_DimCausali = caus.id_DimCausali
    left outer join {{ ref('stg_dim_albero') }} alb
        on ft.id_DimAlbero = alb.id_DimAlbero
    left outer join {{ ref('stg_dim_contratti_prestazione') }} con
        on ft.id_DimContrattiPrestazione = con.id_DimContrattiPrestazione
    left outer join {{ ref('stg_dim_clienti') }} cli
        on ft.id_DimClienti = cli.id_DimClienti
    left outer join {{ ref('stg_dim_contratti_fornitura') }} forn
        on con.cf = forn.bk_ContrattoFornitura
    cross apply {{ ref('udf_specialty') }} spe
    left join {{ ref('stg_specialty_adjustments') }} spe_adj
        on spe.DESC_SPECIALTY = spe_adj.DESC_SPECIALTY
    where
        ft.id_DimDataDocumento = 20250101 -- @PeriodoGiorno
        and pr.IdProdottoCDG = 'Temps'
        and alb.Division in ('DIV_RIT', 'DIV_RIH')
        and s.bk_Societa = 'Randstad'
        and con.CodiceLavoratore is not null
        and coalesce(con.MansioneCodice, '') = spe.cod_mansione
),

cteCosti as (
    select
        substring(cast(costi.id_DimDataDocumento as string), 1, 4) as accounting_year,
        substring(cast(costi.id_DimDataDocumento as string), 5, 2) as accounting_period,
        con.CodiceLavoratore as codicelavoratore,
        cli.bk_CodiceContabilie as nr_cliente,
        cast(alb.bk_Unit as string) as client_number_2,
        con.bk_CP as nr_contratto_prestazione,
        costi.CostoTotale as costo_totale,
        alb.Division || '_' || coalesce(spe_adj.DESC_SPECIALTY, spe.DESC_SPECIALTY) || '_TEMP' as division_code,
        coalesce(con.MansioneCodice, '') as position,
        con.MatricolaGenerale as matricolagenerale,
        replace(coalesce(cast(con.Datalnizio as string), '19000101'), '-', '') as start_date_job_order,
        forn.TariffaOraOrdinaria as client_rate
    from {{ ref('stg_vw_fact_costo_totale_per_cp') }} costi
    inner join {{ ref('stg_dim_prodotto_cdg') }} pr
        on costi.id_DimProdottoCDG = pr.IdProdottoCDG
    inner join {{ ref('stg_dim_societa') }} s
        on costi.id_DimSocieta = s.Id_DimSocieta
    left outer join {{ ref('stg_dim_causali') }} caus
        on costi.id_DimCausali = caus.id_DimCausali
    left outer join {{ ref('stg_dim_albero') }} alb
        on costi.id_DimAlbero = alb.id_DimAlbero
    left outer join {{ ref('stg_dim_contratti_prestazione') }} con
        on costi.id_DimContrattiPrestazione = con.id_DimContrattiPrestazione
    left outer join {{ ref('stg_dim_clienti') }} cli
        on costi.id_DimClienti = cli.id_DimClienti
    left outer join {{ ref('stg_dim_contratti_fornitura') }} forn
        on con.cf = forn.bk_ContrattoFornitura
    cross apply {{ ref('udf_specialty') }} spe
    left join {{ ref('stg_specialty_adjustments') }} spe_adj
        on spe.DESC_SPECIALTY = spe_adj.DESC_SPECIALTY
    where
        costi.id_DimDataDocumento = 20250101 -- @PeriodoGiorno
        and pr.IdProdottoCDG = 'Temps'
        and alb.Division in ('DIV_RIT', 'DIV_RIH')
        and s.bk_Societa = 'Randstad'
        and con.CodiceLavoratore is not null
        and coalesce(con.MansioneCodice, '') = spe.cod_mansione
),

cteFT2 as (
    select
        accounting_year,
        accounting_period,
        codicelavoratore,
        nr_cliente,
        client_number_2,
        nr_contratto_prestazione,
        division_code,
        position,
        sum(importo_totale_fp) as importo_totale_fp,
        sum(case when new_statistic_group in (1, 2, 3, 8, 11) then importo_totale_fp else 0 end) as somma_fp_per_nsg,
        max(start_date_job_order) as start_date_job_order,
        max(client_rate) as client_rate
    from cteFT
    group by
        accounting_year,
        accounting_period,
        codicelavoratore,
        nr_cliente,
        client_number_2,
        nr_contratto_prestazione,
        division_code,
        position
),

cteCosti2 as (
    select
        accounting_year,
        accounting_period,
        codicelavoratore,
        nr_cliente,
        client_number_2,
        nr_contratto_prestazione,
        division_code,
        position,
        sum(costo_totale) as costo_totale,
        max(matricolagenerale) as matricolagenerale
    from cteCosti
    group by
        accounting_year,
        accounting_period,
        codicelavoratore,
        nr_cliente,
        client_number_2,
        nr_contratto_prestazione,
        division_code,
        position
),

tblAll as (
    select
        coalesce(ft.accounting_year, c.accounting_year) as accounting_year,
        coalesce(ft.accounting_period, c.accounting_period) as accounting_period,
        coalesce(ft.codicelavoratore, c.codicelavoratore) as codicelavoratore,
        coalesce(ft.nr_cliente, c.nr_cliente) as nr_cliente,
        coalesce(ft.client_number_2, c.client_number_2) as client_number_2,
        coalesce(ft.nr_contratto_prestazione, c.nr_contratto_prestazione) as nr_contratto_prestazione,
        coalesce(ft.division_code, c.division_code) as division_code,
        coalesce(ft.position, c.position) as position,
        coalesce(ft.client_rate, c.client_rate) as client_rate,
        coalesce(ft.somma_fp_per_nsg, 0) as somma_fp_per_nsg,
        coalesce(ft.importo_totale_fp, 0) as importo_totale_fp,
        c.matricolagenerale,
        coalesce(c.costo_totale, 0) as costo_totale
    from cteFT2 ft
    full outer join cteCosti2 c
        on ft.accounting_year = c.accounting_year
        and ft.accounting_period = c.accounting_period
        and ft.codicelavoratore = c.codicelavoratore
        and ft.nr_cliente = c.nr_cliente
        and ft.client_number_2 = c.client_number_2
        and ft.nr_contratto_prestazione = c.nr_contratto_prestazione
        and ft.division_code = c.division_code
        and ft.position = c.position
),

tblAllPulita as (
    select
        t.*,
        co.bk_CustomerNo
    from tblAll t
    inner join {{ source('dwh_clean_data', 'DimContatti') }} co
        on t.nr_cliente = co.bk_CustomerNo
),

pre_final_select as (
    select
        record_type,
        interface_file_name,
        mutation_code,
        country_code,
        division_code,
        timeframe_code,
        accounting_year,
        accounting_period,
        employee_number_1,
        client_number_1,
        client_number_2,
        client_number_3,
        function_code,
        employee_number_2,
        employee_number_3,
        filler,
        filler2,
        filler3,
        filler4,
        filler5,
        filler6,
        number_of_timesheets,
        total_sales_amount,
        sales_amount_hours,
        direct_costs,
        start_date_job_order,
        client_rate,
        coalesce(pf.Stipendio / nullif(pf.HHRetr, 0), 0) as employee_rate,
        coalesce(client_rate / nullif( (coalesce(pf.Stipendio, 0) / nullif(coalesce(pf.HHRetr, 0), 0)), 0), 0) as tariff_coefficient,
        coalesce(pf.HHLav, 0) as hours_invoiced,
        coalesce(pf.HHRetr, 0) as hours_payrolled
    from (
        select
            'D' as record_type,
            'IBOZIF' as interface_file_name,
            'I' as mutation_code,
            'IT' as country_code,
            division_code,
            'M' as timeframe_code,
            accounting_year,
            accounting_period,
            codicelavoratore as employee_number_1,
            nr_cliente as client_number_1,
            client_number_2,
            nr_contratto_prestazione as client_number_3,
            position as function_code,
            '' as employee_number_2,
            '' as employee_number_3,
            '' as filler,
            '' as filler2,
            '' as filler3,
            '' as filler4,
            '' as filler5,
            '' as filler6,
            '0000001' as number_of_timesheets,
            somma_fp_per_nsg as total_sales_amount,
            somma_fp_per_nsg as sales_amount_hours,
            costo_totale as direct_costs,
            start_date_job_order,
            client_rate,
            matricolagenerale,
            nr_cliente
        from tblAllPulita
    ) as subquery
    left outer join {{ ref('stg_dwh__paghe_fatture') }} pf
        on subquery.matricolagenerale = cast(pf.Matricola as string)
    where
        subquery.nr_cliente is not null
),

final_select as (
    select
        record_type,
        interface_file_name,
        mutation_code,
        country_code,
        division_code,
        timeframe_code,
        accounting_year,
        accounting_period,
        employee_number_1,
        client_number_1,
        client_number_2,
        client_number_3,
        function_code,
        employee_number_2,
        employee_number_3,
        filler,
        filler2,
        filler3,
        filler4,
        filler5,
        filler6,
        number_of_timesheets,
        {{ format_number(total_sales_amount, 13, 2, 1) }} as total_sales_amount,
        {{ format_number(sales_amount_hours, 13, 2, 1) }} as sales_amount_hours,
        {{ format_number(direct_costs, 13, 2, 1) }} as direct_costs,
        start_date_job_order,
        {{ format_number(client_rate, 5, 2, 1) }} as client_rate,
        {{ format_number(employee_rate, 5, 2, 1) }} as employee_rate,
        {{ format_number(tariff_coefficient, 5, 2, 1) }} as tariff_coefficient,
        {{ format_number(hours_invoiced, 7, 2, 0) }} as hours_invoiced,
        {{ format_number(hours_payrolled, 7, 2, 0) }} as hours_payrolled
    from pre_final_select
)

select * from final_select
