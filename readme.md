Meltano approach, 

- download CNX 500 companies from NSE (meltano) in postgres
- from postgres table put the company details to a google sheet (meltano)
- perform analytics in google sheet to, 
    - Exclude stocks with price < 20 and > 50000 (dbt)
    - Order stocks with least difference between high and LAP (dbt)
    - Select Top 10  z

PGSQL approach,
- Import NSE CSV file into postgresql (ref: https://dataschool.com/learn-sql/importing-data-from-csv-in-postgresql/)
- Write stored procedure or python program that, 
    - download 52 week high (high) , last adjusted price (LAP) for each of these companies using pgsql HTTP Client (https://github.com/pramsey/pgsql-http)
    - Exclude stocks with price < 20 and > 50000 (dbt)
    - Order stocks with least difference between high and LAP (dbt)
    - Select Top 10  

    - create table cnx500companies (company varchar(200), industry varchar(25), symbol varchar(25), series varchar(5), Isin varchar(25),ltp money, yearlyhigh money);

    - copy cnx500companies (company,industry,symbol, series,isin)
       from 'd:\wl-data\projects\momentumflow\ind_nifty500list.csv'
       delimiter ',' CSV header;
    - 