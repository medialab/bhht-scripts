import csv
import json
from minet import multithreaded_fetch
from tqdm import tqdm
from collections import defaultdict

PARALLELISM = 1
GROUPS = 50
LIMIT = None

REPORT_HEADERS = ['lang', 'name', 'timestamp']

URL_TEMPLATE = 'https://%s.wikipedia.org/w/api.php?action=query&prop=revisions&rvlimit=1&rvprop=timestamp&rvdir=newer&titles=%s&format=json'

def chunker(iterator, size=GROUPS):
    chunk = []

    for item in iterator:
        if len(chunk) == size:
            yield chunk
            chunk = []
        chunk.append(item)

    yield chunk

def format_url(lang, name):
    return URL_TEMPLATE % (lang, name)

def extract_timestamp(response):
    try:
        data = json.loads(response.data.decode())
        pages = data['query']['pages']
        page = next(iter(pages.values()))

        return page['revisions'][0]['timestamp']
    except:
        return None

PAGES = defaultdict(list)
with open('./missing_creationdate.csv') as f:
    reader = csv.reader(f)
    next(reader)

    i = 0
    for line in tqdm(reader, desc='Reading CSV file'):
        i += 1
        PAGES[line[0]].append(line[1])

        if LIMIT is not None and i >= LIMIT:
            break

with open('./creationdates.csv', 'w') as output_file:
    writer = csv.writer(output_file)
    writer.writerow(REPORT_HEADERS)

    for lang, names in sorted(PAGES.items(), key=lambda x: len(x[1]), reverse=True):
        urls = ((lang, name, format_url(lang, name)) for name in names)
        missing = 0

        key = lambda x: x[2]

        loading_bar = tqdm(desc='Fetching %s' % lang, total=len(names))

        for result in multithreaded_fetch(urls, domain_parallelism=PARALLELISM, key=key):
            loading_bar.update()
            timestamp = extract_timestamp(result.response)

            lang, name, url = result.item

            # assert timestamp is not None, 'Missing timestamp for %s %s %s' % result.item
            if timestamp is None:
                missing += 1
                loading_bar.set_postfix(missing=missing)

            writer.writerow([lang, name, timestamp or ''])
