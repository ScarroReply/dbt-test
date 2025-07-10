with cteA as (
    select
        'D' as record_type,
        'IBDIIF' as interface_file_name,
        'I' as mutation_code,
        'IT' as country_code,
        'DIV_RIH' as division_code,
        'Randstad in House' as division_name
    union all
    select
        'D' as record_type,
        'IBDIIF' as interface_file_name,
        'I' as mutation_code,
        'IT' as country_code,
        'DIV_RIT' as division_code,
        'Randstad Italia' as division_name
),

cteB as (
    select
        A.*,
        A.division_code || '_' || S.desc_specialty || '_TEMP' as division_code_specialty,
        A.division_name || ' ' || S.desc_estesa_specialty as division_name_specialty
    from cteA A
    cross join {{ ref('udf_specialty') }} S
    where A.division_code = S.division_code
),

final as (
    select
        record_type,
        interface_file_name,
        'U' as mutation_code, -- Simula l'update
        country_code,
        division_code_specialty as division_code,
        division_name_specialty as division_name,

        -- 4. Aggiornamento finale (campo Report Record)
        record_type ||
        interface_file_name ||
        'U' || -- Mutation Code
        country_code ||
        division_code_specialty ||
        division_name_specialty as report_record

    from cteB
)

select * from final