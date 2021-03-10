from nsetools import Nse
from contextlib import closing
import requests
import codecs
import csv

# 1. On the first trading day of the month, we rank all stocks based on their “Naive
# Momentum”: Absolute return over the last 52 weeks (250 working days)
# 2. Buy the top 20 stocks in equal weight from this list. Long only.
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
        counter += 1
        # if counter > 100:
        #     break

orderedList = sorted(symbolList.items(), key=lambda kv: kv[1], reverse=True)
for i in orderedList[:20]:
    print(i)
