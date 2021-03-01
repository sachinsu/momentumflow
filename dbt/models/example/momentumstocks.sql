with
    cnxcompanies
    as
    (

        select
            symbol,
            company,
            ltp,
            yearlyhigh,
            updatedat

        from cnx500companies
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
    'buy' as buyorsell
from cnxcompanies
order by yearlyhigh-ltp 
    limit 20

)

select * from cnxtopstocks