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

INPUT = './final.csv'
OUTPUT = './final-clustering.csv'

FIELDNAMES_TO_ADD = [
    'transliteration',
    'clustering_1_normalization',
    'clustering_1_normalization_confidence'
]

def process(name):
    return name.lower().strip()

def process_harsher(name):
    return squeeze(process(name), keep_roman_numerals=True).replace('-', '').replace('_', '')

CONFIDENCE_TOTAL = 4.0
def confidence_score(cluster):
    score = 0

    # 1. Gender
    genders = set(r['gender_B'] for r in cluster if r['gender_B'])

    # If different genders are recorded we dismiss the cluster
    if len(genders) > 1:
        return 0

    score += len([r for r in cluster if r['gender_B']]) / len(cluster)

    # 2. Birth date
    births = set(r['birth_B'] for r in cluster if r['birth_B'])

    if len(births) > 1:
        return 0

    score += len([r for r in cluster if r['birth_B']]) / len(cluster)

    # 3. Death date
    deaths = set(r['death_B'] for r in cluster if r['death_B'])

    if len(deaths) > 1:
        return 0

    score += len([r for r in cluster if r['death_B']]) / len(cluster)

    # 4. Occupation
    score += 0.5 * (1 if len(set(r['final_occupation_L2_B'] for r in cluster)) == 1 else 0)

    # 5. Citizenship
    score += 0.5 * (1 if len(set(r['final_citizenship'] for r in cluster)) == 1 else 0)

    return score / CONFIDENCE_TOTAL


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

DATA = []

with codecs.open(INPUT, encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    buckets = defaultdict(list)

    for line in tqdm(itertools.islice(reader, 0, 10000)):
        line['transliteration'] = unidecode(line['name'])
        DATA.append(line)

# 1. Basic normalization
basic_normalization = lambda i: process(DATA[i]['transliteration'])

for c, cluster in enumerate(key_collision(range(len(DATA)), key=basic_normalization)):
    items = [DATA[i] for i in cluster]

    confidence = confidence_score(items)

    for item in items:
        item['clustering_1_normalization'] = c
        item['clustering_1_normalization_confidence'] = confidence

print('Found %i clusters for `clustering_1_normalization`' % (c + 1))

with open(OUTPUT, 'w') as of:
    writer = csv.DictWriter(of, fieldnames=fieldnames + FIELDNAMES_TO_ADD)
    writer.writeheader()

    for item in DATA:
        writer.writerow(item)
