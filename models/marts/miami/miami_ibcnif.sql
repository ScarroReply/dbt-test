with udf_dati_ibcnif as (
    select
        'D' as record_type,
        'IBCNIF' as interface_file_name,
        'I' as mutation_code,
        'IT' as country_code,
        CodInternazionale as concern_code,
        ConcernName as concern_name,
        '' as filler,
        cast(ConcernLevel as string) as concern_level,
        cast(UpperLevel as string) as upper_level,
        '' as report_record
    from {{ ref('stg_tbl_gruppi_clienti') }}
    where CodInternazionale is not null and ConcernName is not null
),

cteA as (
    select *, 'DIV_RIH' as division_code from udf_dati_ibcnif
    union all
    select *, 'DIV_RIT' as division_code from udf_dati_ibcnif
    union all
    select *, 'DIV_MAG' as division_code from udf_dati_ibcnif
    union all
    select *, 'DIV_MIN' as division_code from udf_dati_ibcnif
),

cteB as (
    select
        cteA.*,
        s.desc_specialty,
        cteA.division_code || '_' || s.desc_specialty || '_TEMP' as division_code_specialty
    from cteA
    left join {{ ref('udf_specialty') }} s
        on cteA.division_code = s.division_code
),

final as (
    select
        record_type,
        interface_file_name,
        country_code,
        division_code_specialty as division_code,
        concern_code,
        'U' as mutation_code,
        concern_name,
        concern_level,
        upper_level,
        record_type || 
        interface_file_name || 
        'U' || 
        country_code || 
        division_code_specialty || 
        concern_code || 
        concern_name || 
        '' || 
        concern_level || 
        coalesce(upper_level, '') as report_record

    from cteB
)

select * from final
