with source_mansioni as (
    select * from {{ ref('stg_tbl_mansioni') }}
),

udf_specialty_data as (
    select * from {{ ref('udf_specialty') }}
)

select distinct
    'D' as record_type,
    'IBFUIF' as interface_file_name,
    'I' as mutation_code,
    'IT' as country_code,
    -- La division_code viene passata come parametro alla funzione originale, qui la gestiremo nel modello chiamante
    source_mansioni.C_MANIST_COD as function_code,
    coalesce(left(source_mansioni.C_MANIST_DES, 50), '') as function_description,
    coalesce(source_mansioni.C_RAGGRMANS_COD, '') as function_group,
    '' as function_group_description, -- Non presente nella udf, ma nel PDF
    '' as filler,
    replace(source_mansioni.C_ISTAT_COD, '.', '') as ilo_code,
    '' as report_record -- Verrà calcolato nel modello finale
from source_mansioni
cross join udf_specialty_data s
where source_mansioni.C_MANIST_COD = s.cod_mansione
  and s.division_code = 'DIV_RIH' -- Questo sarà il parametro @DivisionCode
