with cte as (
    select 'DIV_RIT' as division_code
    union all
    select 'DIV_RIH' as division_code
    union all
    select 'DIV_INT' as division_code
),

main_logic as (
    select
        t1.cod_mansione_randstad_italy as cod_mansione,
        t1.desc_mansione_randstad_italy as desc_mansione,
        case
            when cte.division_code = 'DIV_INT' then 'OP'
            else
                case
                    when t2.specialty = 'digital' then 'DI'
                    when t2.specialty = 'operational' then 'OP'
                    when t2.specialty = 'professional' then 'PR'
                    else ''
                end
        end as desc_specialty,
        case
            when cte.division_code = 'DIV_INT' then 'operational'
            else
                case
                    when t2.specialty = 'digital' then 'digital'
                    when t2.specialty = 'operational' then 'operational'
                    when t2.specialty = 'professional' then 'professional'
                    else ''
                end
        end as desc_estesa_specialty,
        cte.division_code
    from {{ ref('stg_mansioni_trascodifica_rnd') }} t1
    left join {{ ref('stg_mansioni_rasco_isco_spec') }} t2
        on t2.code_6_dgt_rasco = t1.cod_mansione_randstad_italy
    cross join cte
),

union_all_logic as (
    select * from main_logic
    union all
    select
        '' as cod_mansione,
        '' as desc_mansione,
        case
            when division_code = 'DIV_INT' then 'OP'
            else 'OP'
        end as desc_specialty,
        case
            when division_code = 'DIV_INT' then 'operational'
            else 'operational'
        end as desc_estesa_specialty,
        division_code
    from cte
)

select * from union_all_logic
