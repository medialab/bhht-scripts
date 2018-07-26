import csv
from tqdm import tqdm

DATA = './base1_individus.csv'
RESULT = './clustering_results.csv'
FIXED_RESULT = './fixed_clustering_results.csv'

INDEX = {}

with open(DATA, 'r') as f:
    reader = csv.DictReader(f)

    for line in tqdm(reader, 'Reading CSV data', total=1846130):
        INDEX[line['id']] = line['language']

with open(RESULT, 'r') as rf, \
     open(FIXED_RESULT, 'w') as of:
    reader = csv.DictReader(rf)
    writer = csv.DictWriter(of, fieldnames=reader.fieldnames)
    writer.writeheader()

    for line in reader:
        ids = ['%s:%s' % (i, INDEX[i]) for i in line['ids'].split('|')]
        line['ids'] = '|'.join(ids)

        writer.writerow(line)
