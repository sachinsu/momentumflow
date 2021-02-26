from contextlib import closing
import requests
import codecs
import csv
from datetime import datetime, timezone
from lxml import html
import singer
# from singer import metadata

LOGGER = singer.get_logger()


# class YfHelper():
#     def __init__(self, ):
#         self.cnxurl = cnxurl
#         self.yfurl = yfurl

#     @classmethod
def getData(cnxurl, yfurl):
    LOGGER.info("Getting data with %s\t%s", cnxurl,yfurl)
    # stockdata = []
    counter = 0
    with closing(requests.get(cnxurl, stream=True)) as r:
        reader = csv.reader(codecs.iterdecode(
            r.iter_lines(), 'utf-8'), delimiter=',', quotechar='"')
        # skip the header row
        next(reader)
        for row in reader:
            # if counter > 2:
            #     break
            # else:
            #     counter = counter + 1

            now = datetime.now(timezone.utc).isoformat()
            # Get ticker data
            
            LOGGER.info("Data for %s [%s]",row[0], row[2])
            page = requests.get(
                yfurl + row[2] + '.NS')
            tree = html.fromstring(page.content)

            ltp = tree.xpath(
                '//*[@id="quote-header-info"]/div[3]/div[1]/div/span[1]/text()')[0]

            ltp = ltp.replace(",","")

            yearlyhigh = tree.xpath(
                '//*[@id="quote-summary"]/div[1]/table/tbody/tr[6]/td[2]/text()')[0].split('-')[1]

            yearlyhigh = yearlyhigh.replace(",","")

                # singer.write_schema('cnx_stock', schema, 'timestamp')
                # singer.write_records(
            # stockdata.append({'timestamp': now, 'company': row[0], 'symbol': row[2], 'ltp': float(ltp), 'yearlyhigh': float(yearlyhigh)})
            yield {'timestamp': now, 'company': row[0], 'symbol': row[2], 'ltp': float(ltp), 'yearlyhigh': float(yearlyhigh)}

    # return stockdata