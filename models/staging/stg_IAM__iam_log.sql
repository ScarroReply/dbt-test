-- models/staging/stg_IAM__iam_log.sql

select
    -- {{ dbt_utils.generate_surrogate_key(['IdFile', 'Period']) }} as iam_log_id, chiave surrogata
    IdFile as id_file,
    InsertDate as insert_date,
    Period as period

from {{ source('iam_log', 'IAM_LOG') }}