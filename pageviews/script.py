#!/usr/bin/env python3
# =========================
# BHHT Wikipedia API Script
# =========================
#
# Simple script aiming at hitting the Wikipedia analytics API to gather
# some data about page visits.
#
from pathlib import Path
from collections import defaultdict
import asyncio
import aiohttp
import aiofiles
import csv
import json

import helpers

# Parameters
START_DATE = 20160101
END_DATE = 20161231
CHUNK_SIZE = 10
CONCURRENCY = CHUNK_SIZE

# Constants
WIKIPEDIA_PAGE_VIEW_URL = 'http://wikimedia.org/api/rest_v1/metrics/pageviews/per-article'
WIKIPEDIA_PAGE_VIEW_TEMPLATE = '%(url)s/%(lang)s.wikipedia/all-access/user/%(name)s/monthly/%(start_date)s00/%(end_date)s00'

# State
COMPUTED_PAGES = set()
DONE_COUNT = 0

# Helper used to write a single CSV line (does not need to take edge cases)
def write_csv_line(rows):
    rows = map(str, rows)
    return ','.join('"' + row + '"' if ',' in row else row for row in rows)

# Function hashing the given row
def hash_row(row):
    return row['name'] + '§' + row['lang']

# Generator consuming the input file
def input_file_generator(file_path):
    with open(file_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            yield row

# Coroutine used to download one page's data
async def get_page_stats(session, row):

    # Building URL
    url_params = {
        'url': WIKIPEDIA_PAGE_VIEW_URL,
        'lang': row['lang'],
        'name': row['name'],
        'start_date': START_DATE,
        'end_date': END_DATE
    }

    url = WIKIPEDIA_PAGE_VIEW_TEMPLATE % url_params

    async with session.get(url) as response:

        # Hitting the rate limit
        if response.status == 429:
            print('We hit the rate limit!!!')
            return None

        if response.status >= 500:
            print('Server error from wikipedia!!!')
            return None

        # Reading response
        text = await response.text()

        # Attempting to parse API result
        try:

            # Parsing JSON
            data = json.loads(text)
            items = data['items'] if 'items' in data else []

            # Filling empty months data
            monthsIndex = defaultdict(int)

            for item in items:
                monthsIndex[int(item['timestamp'][4:6])] = item['views']

        except Exception as e:
            print('Error', e, url)
            return None

        return [monthsIndex[month] for month in range(1, 13)]

# Function used to process one single page
async def process_page(session, semaphore, output, row):
    global DONE_COUNT

    print('(%i) Processing "%s"...' % (DONE_COUNT, row['name']))

    # Fetching data from API
    async with semaphore:
        months = await get_page_stats(session, row)

    if months:
        DONE_COUNT += 1

        # Writing result as CSV
        csv_line = write_csv_line([
            row['lang'],
            row['id'],
            row['name']
        ] + months)

        await output.write(csv_line + '\n')

# Main loop
async def main(loop, output_file_path, input_file_path):
    input_file = input_file_generator(input_file_path)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiofiles.open(output_file_path, encoding='utf-8', mode='a+') as o,\
               aiohttp.ClientSession(loop=loop) as session:

        chunk = helpers.consume(input_file, CHUNK_SIZE)

        while len(chunk):

            tasks = []
            for row in chunk:

                if 'lang' not in row:
                    row['lang'] = 'en'

                # Skip if already done
                if hash_row(row) in COMPUTED_PAGES:
                    continue

                tasks.append(process_page(session, semaphore, o, row))

            if tasks:
                await asyncio.wait(tasks)

            # Next chunk
            chunk = helpers.consume(input_file, CHUNK_SIZE)

# Launching process if script is invoked as main
if __name__ == '__main__':
    import argparse

    # Parsing command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='Path to the input file.')
    parser.add_argument('output', help='Path to the output file.')
    args = parser.parse_args()

    # Touching result file
    Path(args.output).touch()

    # Checking which part we already did
    with open(args.output, encoding='utf-8', mode='r') as f:
        reader = csv.reader(f)

        for row in reader:
            row = {
                'name': row[2],
                'lang': row[0]
            }

            COMPUTED_PAGES.add(hash_row(row))

    DONE_COUNT = len(COMPUTED_PAGES)

    print('Already done %i pages.' % DONE_COUNT)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, args.output, args.input))
