select
    bk_CodiceContabilie
from {{ ref('stg_dim_clienti') }}
where bk_CodiceContabilie = '_BK_CodiceContabilie' -- Questo sarà un parametro passato alla funzione
