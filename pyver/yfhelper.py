from contextlib import closing
import requests
import singer
import codecs
import csv
from datetime import datetime, timezone
from lxml import html

schema = {
    'properties':   {
        'company': {'type': 'string'},
        'symbol': {'type': 'string'},
        'timestamp': {'type': 'string', 'format': 'date-time'},
        'ltp': {'type': 'number'},
        'yearlyhigh': {'type': 'number'},
    },
}

url = "https://www1.nseindia.com/content/indices/ind_nifty500list.csv"

with closing(requests.get(url, stream=True)) as r:
    reader = csv.reader(codecs.iterdecode(
        r.iter_lines(), 'utf-8'), delimiter=',', quotechar='"')
    for row in reader:
       # Handle each row here...
        now = datetime.now(timezone.utc).isoformat()
        # Get ticker data
        page = requests.get(
            'https://finance.yahoo.com/quote/' + row[2] + '.NS')
        tree = html.fromstring(page.content)

        ltp = tree.xpath(
            '//*[@id="quote-header-info"]/div[3]/div[1]/div/span[1]/text()')[0]

        yearlyhigh = tree.xpath(
            '//*[@id="quote-summary"]/div[1]/table/tbody/tr[6]/td[2]/text()')[0].split('-')[1]

        singer.write_schema('cnx_stock', schema, 'timestamp')
        singer.write_records(
            'cnx_stock', [{'timestamp': now, 'company': row[0], 'symbol': row[2], 'ltp': float(ltp), 'yearlyhigh': float(yearlyhigh)}])
