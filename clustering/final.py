import csv
import codecs
import itertools
from tqdm import tqdm
from collections import defaultdict
from unidecode import unidecode
from fog.clustering import passjoin
from fog.phonetics import cologne, rusalka
from fog.utils import squeeze
from fog.key import fingerprint

INPUT = './final.csv'
OUTPUT = './final-clusters-fingerprint.csv'

def process(name):
    return unidecode(name.lower()).strip()

def process_harsher(name):
    return squeeze(process(name)).replace('-', '').replace('_', '')

def sensible(cluster):
    genders = set(r['gender_B'] for r in cluster if r['gender_B'])

    if len(genders) > 1:
        return False

    births = set(r['birth_B'] for r in cluster)

    if len(births) == 1 and '' not in births:
        return True

    deaths = set(r['death_B'] for r in cluster)

    if len(deaths) == 1 and '' not in deaths:
        return True

    # occupations = set(r['final_occupation_L2_B'] for r in cluster)

    # if len(occupations) == 1 and '' not in occupations:
    #     return True

    return False

with codecs.open(INPUT, encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    buckets = defaultdict(list)

    for line in tqdm(reader):
        processed_name = fingerprint(line['name'])
        buckets[processed_name].append(line)

print(sum(1 if len(b) > 1 else 0 for b in buckets.values()))
print(sum(1 if len(b) > 1 else 0 for b in buckets.values() if sensible(b)))

for b in itertools.islice((b for b in buckets.values() if len(b) > 1), 0, 25):
    print()

    for line in b:
        print(line['group_PAPER_B'], line['name'])

with open(OUTPUT, 'w') as of:
    writer = csv.DictWriter(of, fieldnames=fieldnames + ['cluster'])
    writer.writeheader()

    for i, cluster in enumerate(b for b in buckets.values() if len(b) > 1):

        # BEWARE: For harsher purposes
        if len(set(process_harsher(row['name']) for row in cluster)) < 2:
            continue

        for row in cluster:
            row['cluster'] = i
            writer.writerow(row)
