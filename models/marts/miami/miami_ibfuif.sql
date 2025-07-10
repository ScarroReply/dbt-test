with udf_dati_ibfuif_data as (
    select * from {{ ref('udf_dati_ibfuif') }}
),

cteA as (
    select *, 'DIV_RIT' as division_code_param from udf_dati_ibfuif_data
    union all
    select *, 'DIV_RIH' as division_code_param from udf_dati_ibfuif_data
),

final as (
    select
        record_type,
        interface_file_name,
        'U' as mutation_code,
        country_code,
        division_code_param as division_code,
        function_code,
        function_description,
        function_group,
        function_group_description,
        filler,
        ilo_code,
        record_type ||
        interface_file_name ||
        'U' ||
        country_code ||
        division_code_param ||
        coalesce(function_code, '') ||
        coalesce(function_description, '') ||
        coalesce(function_group, '') ||
        coalesce(function_group_description, '') ||
        filler ||
        coalesce(ilo_code, '') as report_record

    from cteA
)

select * from final