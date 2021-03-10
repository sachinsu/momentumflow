from nsetools import Nse
from contextlib import closing
import requests
import codecs
import csv

# 1. On the first trading day of the month, we rank all stocks based on their
# “Volatility-Adjusted Momentum”: Absolute return over the last 52 weeks (250 working
# days) divided by the annualized standard deviation of daily price moves
# 2. Buy the top 30 stocks in equal weight from this list. Long only.
# 3. Rebalance every month


symbolList = {}
nse = Nse()

cnxurl = 'https://www1.nseindia.com/content/indices/ind_nifty500list.csv'
# counter = 0

with closing(requests.get(cnxurl, stream=True)) as r:
    reader = csv.reader(codecs.iterdecode(
        r.iter_lines(), 'utf-8'), delimiter=',', quotechar='"')
    # skip the header row
    next(reader)
    for row in reader:
        symbol = row[2]
        print(symbol)
        p = nse.get_quote(symbol)
        todaysprice = p["dayHigh"]
        lowyearly = p["low52"]
        absolutereturns = (todaysprice-lowyearly)/lowyearly
        symbolList[symbol] = absolutereturns

        # TODO: Calculated Annualized Volatility
        # STEP 1: Calculate Daily Returns, we know that the daily returns can be calculated as –
        # Return = LN (Ending Price / Beginning Price), where LN denotes Logarithm to Base ‘e’, note this is also called ‘Log Returns’.
        # STEP 2: calculate standard deviation for the daily returns --> this will give daily volatility
        # Step 3: Calculate annual volatility as Daily Volatility * SQRT(252)
        # STEP 4: Divide Absolutereturns by Annual Volatility

        # counter += 1
        # if counter > 100:
        #     break

orderedList = sorted(symbolList.items(), key=lambda kv: kv[1], reverse=True)
for i in orderedList[:20]:
    print(i)
