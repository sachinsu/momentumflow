from contextlib import closing
import requests
import codecs
import csv
from datetime import datetime, timezone
from lxml import html

import singer
from singer import metadata

LOGGER = singer.get_logger()


class YfHelper():
    config = {}
    state = {}
    catalog = {}
    stream_map = {}
    stream_objects = {}
    counts = {}

    @classmethod
    def getData(cls):
        cnxurl = string(cls.config.get("cnx500fileurl"))
        yfurl = string(cls.config.get("yfinanceurl"))

        with closing(requests.get(cnxurl, stream=True)) as r:
            reader = csv.reader(codecs.iterdecode(
                r.iter_lines(), 'utf-8'), delimiter=',', quotechar='"')
            for row in reader:
                now = datetime.now(timezone.utc).isoformat()
                # Get ticker data
                page = requests.get(
                    yfurl + row[2] + '.NS')
                tree = html.fromstring(page.content)

                ltp = tree.xpath(
                    '//*[@id="quote-header-info"]/div[3]/div[1]/div/span[1]/text()')[0]

                yearlyhigh = tree.xpath(
                    '//*[@id="quote-summary"]/div[1]/table/tbody/tr[6]/td[2]/text()')[0].split('-')[1]

                # singer.write_schema('cnx_stock', schema, 'timestamp')
                # singer.write_records(
                # 'cnx_stock', [{'timestamp': now, 'company': row[0], 'symbol': row[2], 'ltp': float(ltp), 'yearlyhigh': float(yearlyhigh)}])

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]
                : s for s in cls.catalog['streams']}
        return cls.stream_map[stream_name]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        stream_metadata = metadata.to_map(stream['metadata'])
        return metadata.get(stream_metadata, (), 'selected')

    @classmethod
    def get_results_per_page(cls, default_results_per_page):
        results_per_page = default_results_per_page
        try:
            results_per_page = int(cls.config.get("results_per_page"))
        except TypeError:
            # None value or no key
            pass
        except ValueError:
            # non-int value
            log_msg = ('Failed to parse results_per_page value of "%s" ' +
                       'as an integer, falling back to default of %d')
            LOGGER.info(log_msg,
                        Context.config['results_per_page'],
                        default_results_per_page)
        return results_per_page
