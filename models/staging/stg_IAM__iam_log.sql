-- models/staging/stg_IAM__iam_log.sql

select
    IdFile,
    InsertDate,
    Period

from {{ source('iam_log', 'IAM_LOG') }}