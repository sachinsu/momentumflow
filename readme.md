Requirements,
- This process has to run every week on Weekend (Sat/Sun)
- download CNX 500 companies from NSE 
- Get LTP and 52 week high from yahoo finance 
- Update this data in a storage (DB)
- Exclude stocks having LTP < 20 & > 50000
- Check if there are any stocks last week that either less than their 5% of 52-week high or not in top 10 , remove such stocks (mark as 'sell') 
- Get list of top 10 stocks which are near their 52-week high and add such that total no of stocks remains 10 (mark as 'buy')

Approach: 

* Meltano for ELT

* Custom tap to get company-wise 52 week high and LTP - done
* Test this tap with postgreSQL (tap-postgres) 
* Dbt to arrive at list of momentum stocks
* (possibly) Airflow to orchestrate on weekly basis