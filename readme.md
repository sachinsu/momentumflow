Requirements,
- This process has to run every week on Weekend (Sat/Sun)
- download CNX 500 companies from NSE 
- Get LTP and 52 week high from yahoo finance 
- Update this data in a storage (DB)
- Exclude stocks having LTP < 20 & > 50000
- Check if there are any stocks last week that either less than their 5% of 52-week high or not in top 10 , remove such stocks (mark as 'sell') 
- Get list of top 10 stocks which are near their 52-week high and add such that total no of stocks remains 10 (mark as 'buy')

Requirement version 2:
1. On the first trading day of the month, we rank all stocks based on their “Naive
Momentum”: Absolute return over the last 52 weeks (250 working days)
2. Buy the top 30 stocks in equal weight from this list. Long only.
3. Rebalance every month


Requirement version 3: 
1. On the first trading day of the month, we rank all stocks based on their
“Volatility-Adjusted Momentum”: Absolute return over the last 52 weeks (250 working
days) divided by the annualized standard deviation of daily price moves
2. Buy the top 30 stocks in equal weight from this list. Long only.
3. Rebalance every month



Approach: 

* Meltano for ELT

* Custom tap to get company-wise 52 week high and LTP - done
* Test this tap with postgreSQL (tap-postgres) 
* Dbt to arrive at list of momentum stocks
* (possibly) Airflow to orchestrate on weekly basis