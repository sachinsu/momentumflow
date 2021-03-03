{{
    config(
        materialized='incremental',
    )
}}

with
    cnxcompanies
    as
    (

        select
            symbol,
            company,
            ltp,
            yearlyhigh,
            updatedat,
            rank() over (order by yearlyhigh-ltp) as diff_rank
        from {{ source('datastore', 'cnx500companies') }}
    where yearlyhigh::money::numeric::float8 - ltp::money::numeric::float8 > 0 and ltp::money::numeric::float8 > 20 and ltp::money::numeric::float8 < 50000

),
 cnxtopstocks as
(

    select
    symbol,
    company,
    ltp,
    yearlyhigh,
    updatedat,
    diff_rank
    from  cnxcompanies
    order by updatedat desc,diff_rank 
)

select * from cnxtopstocks