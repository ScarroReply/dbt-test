-- models/marts/export_miami_report.sql

-- Questo modello finale unisce i record da tutti i modelli IAM
-- per creare l'output finale per Talend, replicando la logica di udf_IAMTable.

with all_records as (

    select report_record from {{ ref('miami_ibcnif') }}

    union all

    select report_record from {{ ref('miami_ibdiif') }}

    union all

    select report_record from {{ ref('miami_ibfuif') }}

    union all

    select report_record from {{ ref('miami_iblkif') }}

    union all

    -- La SP originale filtrava IBOZIF per il periodo corrente.
    -- In dbt, se il modello è incrementale, il filtro viene applicato a monte.
    select report_record from {{ ref('miami_ibozif') }}

),

-- La SP originale aggiungeva un header e un footer.
-- Header
header as (
    select
        'H' || 
        -- format_date('%Y%m%d', current_date) || -- Sintassi per alcuni SQL dialects
        -- format_date('%H%M', current_time) ||
        'DUMMY_HEADER_CONTENT' as final_record,
        1 as sort_order
),

-- Footer
footer as (
    select
        'F' || lpad(cast(count(*) as string), 8, '0') as final_record,
        3 as sort_order
    from all_records
),

-- Dati
body as (
    select 
        report_record as final_record,
        2 as sort_order
    from all_records
    where report_record is not null
),

-- Unione finale con ordinamento
final_report as (
    select final_record from header
    union all
    select final_record from body
    union all
    select final_record from footer
)

select final_record
from final_report
order by sort_order
