{{
    config(
        materialized='incremental',
        unique_key='concat(symbol,updatedat)'
    )
}}

with currentlist as (
    select distinct symbol,
            company,
            ltp,
            yearlyhigh,
            updatedat,diff_rank,'buy' as buyorsell
    from  {{ref('rankstocks')}} 
    order by updatedat desc, diff_rank
    limit 20
),
finallist as (
    {% if is_incremental() %}
        select symbol,
            company,
            ltp,
            yearlyhigh,
            updatedat,diff_rank,'sell' as buyorsell from {{this}} as oldlist
            where not exists (select symbol from currentlist where symbol=oldlist.symbol)
        union 
        select  symbol,
            company,
            ltp,
            yearlyhigh,
            updatedat,diff_rank,'buy' as buyorsell  from  currentlist 
            where not exists (select symbol from {{this}} where symbol=currentlist.symbol and buyorsell='buy')   
    {% else %}
        select * from currentlist
    {% endif %}
)


select * from finallist