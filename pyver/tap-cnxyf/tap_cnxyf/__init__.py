#!/usr/bin/env python3
from contextlib import closing
import requests
import codecs
import csv
from datetime import datetime, timezone
from lxml import html

import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


REQUIRED_CONFIG_KEYS = ["cnx500fileurl", "yfinanceurl"]
LOGGER = singer.get_logger()

CONFIG = {}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key="symbol",
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream: %s, fields %s",
                    stream.tap_stream_id, stream.fields())

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema,
            key_properties=stream.key_properties,
        )

        cnxurl = CONFIG.get("cnx500fileurl")
        yfurl = CONFIG.get("yfinanceurl")

        max_bookmark = None
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

                row = {'timestamp': now, 'company': row[0], 'symbol': row[2], 'ltp': float(
                    ltp), 'yearlyhigh': float(yearlyhigh)}

                singer.write_records(stream.tap_stream_id,  [row])

                if bookmark_column:
                    if is_sorted:
                        # update bookmark to latest value
                        singer.write_state(
                            {stream.tap_stream_id: row[bookmark_column]})
                    else:
                        # if data unsorted, save max value until end of writes
                        max_bookmark = max(max_bookmark, row[bookmark_column])
            if bookmark_column and not is_sorted:
                singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
