import csv
import codecs
import itertools
from tqdm import tqdm
from collections import defaultdict
from unidecode import unidecode
from fog.clustering import passjoin, key_collision
from fog.phonetics import cologne, rusalka
from fog.utils import squeeze
from fog.key import fingerprint
from statistics import mean, median

INPUT = './final.csv'
OUTPUT = './final-clustering.csv'

FIELDNAMES_TO_ADD = [
    'transliteration',
    'clustering_0_exact',
    'clustering_0_exact_confidence',
    'clustering_1_normalization',
    'clustering_1_normalization_confidence',
    'clustering_2_harsher_normalization',
    'clustering_2_harsher_normalization_confidence'
]

def process(name):
    return name.lower().strip()

def process_harsher(name):
    return squeeze(process(name), keep_roman_numerals=True).replace('-', '').replace('_', '')

CONFIDENCE_TOTAL = 4.0
def confidence_score(cluster):
    score = 0

    genders = set(r['gender_B'] for r in cluster)
    genders_without_missing = set(r['gender_B'] for r in cluster if r['gender_B'] and r['gender_B'] != 'Other')

    births = set(r['birth_B'] for r in cluster)
    births_without_missing = set(r['birth_B'] for r in cluster if r['birth_B'])

    deaths = set(r['death_B'] for r in cluster)
    deaths_without_missing = set(r['death_B'] for r in cluster if r['death_B'])

    occupations = set(r['final_occupation_L2_B'] for r in cluster)
    occupations_without_missing = set(r['final_occupation_L2_B'] for r in cluster if r['final_occupation_L2_B'])

    citizenships = set(r['final_citizenship'] for r in cluster)
    citizenships_without_missing = set(r['final_citizenship'] for r in cluster if r['final_citizenship'])

    if (
        len(genders_without_missing) > 1 or
        len(births_without_missing) > 1 or
        len(deaths_without_missing) > 1
    ):
        return 0

    score += 1 - len([r['gender_B'] for r in cluster if not r['gender_B'] or r['gender_B'] == 'Other']) / len(cluster)
    score += 1 - len([r['birth_B'] for r in cluster if not r['birth_B'] or r['birth_B'] == 'Other']) / len(cluster)
    score += 1 - len([r['death_B'] for r in cluster if not r['death_B'] or r['death_B'] == 'Other']) / len(cluster)

    score += 0.5 - len([r['final_occupation_L2_B'] for r in cluster if not r['final_occupation_L2_B'] or r['final_occupation_L2_B'] == 'Other']) / len(cluster) / 2.0
    score += 0.5 - len([r['final_citizenship'] for r in cluster if not r['final_citizenship'] or r['final_citizenship'] == 'Other']) / len(cluster) / 2.0

    return score / CONFIDENCE_TOTAL

DATA = []

with codecs.open(INPUT, encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    buckets = defaultdict(list)

    for line in tqdm(itertools.islice(reader, 0, 10000)):
        line['transliteration'] = unidecode(line['name'])
        DATA.append(line)

def apply_clustering(method, data):
    name = method.__name__

    I = 0
    C = []
    for c, cluster in enumerate(method(range(len(DATA)))):
        items = [DATA[i] for i in cluster]
        I += len(items)

        confidence = confidence_score(items)
        C.append(confidence)

        for item in items:
            item[name] = c
            item[name + '_confidence'] = confidence

    print('`%s` found %i clusters regrouping %i rows (confidence avg: %2f, median: %2f)' % (name, (c + 1), I, mean(C), median(C)))

# 0. Exact
def clustering_0_exact(data):
    return key_collision(data, key=lambda i: DATA[i]['transliteration'])

# 1. Basic normalization
basic_normalization = lambda i: process(DATA[i]['transliteration'])

def clustering_1_normalization(data):
    return key_collision(data, key=basic_normalization)

# Applying clusterings
apply_clustering(clustering_0_exact, DATA)
apply_clustering(clustering_1_normalization, DATA)

with open(OUTPUT, 'w') as of:
    writer = csv.DictWriter(of, fieldnames=fieldnames + FIELDNAMES_TO_ADD)
    writer.writeheader()

    for item in DATA:
        writer.writerow(item)
