import csv
from tqdm import tqdm

INPUT = './base1_individus.csv'
CLUSTERING = './fixed_clustering_results.csv'
OUTPUT = 'tagged.csv'

DUPLICATES = set()

with open(CLUSTERING, 'r') as f:
    reader = csv.DictReader(f)

    for line in reader:

        if line['result'] != 'oui':
            continue

        for i in line['ids'].split('|'):
            DUPLICATES.add(i)

with open(INPUT, 'r') as f, \
     open(OUTPUT, 'w') as o:
     reader = csv.DictReader(f)
     writer = csv.DictWriter(o, fieldnames=reader.fieldnames + ['duplicate'])

     writer.writeheader()

     for line in tqdm(reader):
        line['duplicate'] = '1' if ('%s:%s' % (line['id'], line['language'])) in DUPLICATES else '0'

        writer.writerow(line)
