with udf_dati_iblkif_data as (
    select * from {{ ref('udf_dati_iblkif') }}
),

cteA as (
    select *, 'DIV_RIT' as division_code_param from udf_dati_iblkif_data
    union all
    select *, 'DIV_RIH' as division_code_param from udf_dati_iblkif_data
    union all
    select *, 'DIV_MAG' as division_code_param from udf_dati_iblkif_data
    union all
    select *, 'DIV_MIN' as division_code_param from udf_dati_iblkif_data
),

cte_anomalia as (
    select *
    from cteA
    where (client_number_1 = 'C280261' and concern_code = '028') or client_number_1 <> 'C280261'
),

final as (
    select
        record_type,
        interface_file_name,
        'U' as mutation_code,
        country_code,
        division_code_param as division_code,
        client_number_1,
        client_number_2,
        client_number_3,
        client_name,
        street_name,
        postal_code,
        municipality,
        concern_code,
        client_status,
        date_first_invoice,
        -- Report Record
        record_type || interface_file_name || 'U' || country_code || division_code_param || client_number_1 || client_number_2 || client_number_3 || client_name || street_name || postal_code || municipality || coalesce(concern_code, '') || coalesce(client_status, '') || coalesce(date_first_invoice, '') as report_record

    from cte_anomalia
)

select * from final