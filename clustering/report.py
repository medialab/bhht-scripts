import csv
from collections import defaultdict

CLUSTERING_RESULT = './clustering_results.csv'

CLUSTERS_IDS = defaultdict(list)
IDS_TO_CLUSTERS = defaultdict(list)

print()
print('Clustering report')
print('-' * len('Clustering report'))

with open(CLUSTERING_RESULT, 'r') as f:
    reader = csv.DictReader(f)

    for line in reader:
        key = tuple(sorted(line['ids'].split('|')))

        CLUSTERS_IDS[key].append(line['set'])

        for unique_id in line['ids'].split('|'):
            IDS_TO_CLUSTERS[unique_id].append(line['ids'])

print('1) Sanity tests:')
if any(True for results in CLUSTERS_IDS.values() if len(results) > 1):
    print('  - [WARNING]: Found duplicate duplicates.')
else:
    print('  - No duplicate duplicates.')

print()
