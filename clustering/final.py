import csv
import codecs
import itertools
from tqdm import tqdm
from collections import defaultdict
from unidecode import unidecode
from fog.clustering import passjoin, key_collision, sorted_neighborhood
from fog.phonetics import cologne, rusalka
from fog.utils import squeeze
from fog.key import fingerprint, ngrams_fingerprint
from statistics import mean, median
from Levenshtein import distance as levenshtein

INPUT = './final.csv'
OUTPUT = './final-clustering.csv'

TEST_RUN = True
TEST_RUN_BATCH = 100_000

FIELDNAMES_TO_ADD = [
    'transliteration',
    'valid_cluster'
]

def process(name):
    return name.lower().strip()

def process_harsher(name):
    return squeeze(process(name), keep_roman_numerals=True).replace('-', '').replace('_', '')

def safe_cologne(name):
    try:
        return cologne(name)
    except:
        return None

CONFIDENCE_TOTAL = 4.0
def confidence_score(cluster, boosted=False):
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
        return -1

    score += 1 - len([r['gender_B'] for r in cluster if not r['gender_B'] or r['gender_B'] == 'Other']) / len(cluster)
    score += 1 - len([r['birth_B'] for r in cluster if not r['birth_B'] or r['birth_B'] == 'Other']) / len(cluster)
    score += 1 - len([r['death_B'] for r in cluster if not r['death_B'] or r['death_B'] == 'Other']) / len(cluster)

    if boosted and score == 0:
        score = 0.5

    if len(occupations_without_missing) < 2:
        score += 0.5 - len([r['final_occupation_L2_B'] for r in cluster if not r['final_occupation_L2_B'] or r['final_occupation_L2_B'] == 'Other']) / len(cluster) / 2.0

    if len(citizenships_without_missing) < 2:
        score += 0.5 - len([r['final_citizenship'] for r in cluster if not r['final_citizenship'] or r['final_citizenship'] == 'Other']) / len(cluster) / 2.0

    return score / CONFIDENCE_TOTAL

DATA = []

with codecs.open(INPUT, encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    buckets = defaultdict(list)

    if TEST_RUN:
        reader = itertools.islice(reader, 0, TEST_RUN_BATCH)

    for line in tqdm(reader):
        line['transliteration'] = unidecode(line['name'])
        DATA.append(line)

VALID_CLUSTERS = {}

def apply_clustering(method, data, aggresive=False):
    name = method.__name__

    threshold = 0.75 if not aggresive else 0.8

    FIELDNAMES_TO_ADD.append(name)
    FIELDNAMES_TO_ADD.append(name + '_confidence')

    I = 0
    n = 0
    C = []
    V = 0
    for c, cluster in enumerate(method(range(len(DATA)))):
        items = [DATA[i] for i in cluster]
        I += len(items)
        n += 1

        confidence = confidence_score(items)
        C.append(confidence)

        if confidence >= threshold:
            key = tuple(sorted(cluster))

            if key not in VALID_CLUSTERS:
                VALID_CLUSTERS[key] = name
                V += 1

        for item in items:
            item[name] = c
            item[name + '_confidence'] = confidence

    non_zero_C = [c for c in C if c > 0]
    non_zero_mean = mean(non_zero_C)
    non_zero_median = median(non_zero_C)

    invalid_C = [c for c in C if c == -1]
    valid_C = [c for c in C if c >= threshold]

    print('[%s] %s' % (name, 'aggresive' if aggresive else 'mild'))
    print('  clusters: %i' % n)
    print('  -1 clusters: %i (%2f)' % (len(invalid_C), len(invalid_C) / n))
    print('  >0 clusters: %i' % len(non_zero_C))
    print('  >%s clusters: %i' % (str(threshold), len(valid_C)))
    print('  >%s original clusters: %i' % (str(threshold), V))
    print('  rows: %i' % I)
    print('  confidence avg: %2f *%2f' % (mean(C), non_zero_mean))
    print('  confidence median: %2f *%2f' % (median(C), non_zero_median))
    print()

# 0. Exact
def clustering_0_exact(data):
    return key_collision(data, key=lambda i: DATA[i]['name'])

# 1. Basic normalization
def clustering_1_normalization(data):
    return key_collision(data, key=lambda i: process(DATA[i]['transliteration']))

# 2. Harsher normalization
def clustering_2_harsh_normalization(data):
    return key_collision(data, key=lambda i: process_harsher(DATA[i]['transliteration']))

# 3. Fingerprinting
def clustering_3_fingerprinting(data):
    return key_collision(data, key=lambda i: fingerprint(DATA[i]['transliteration']))

# 4. Bigram fingerprinting
def clustering_4_bigram_fingerprinting(data):
    return key_collision(data, key=lambda i: ngrams_fingerprint(2, DATA[i]['transliteration']))

# 5. Cologne
def clustering_5_cologne(data):
    return key_collision(data, key=lambda i: safe_cologne(DATA[i]['transliteration']))

# 6. Rusalka
def clustering_6_rusalka(data):
    return key_collision(data, key=lambda i: rusalka(DATA[i]['transliteration']))

# 7. SNM k=1
def clustering_7_snm(data):

    distance = lambda i, j: levenshtein(DATA[i]['transliteration'], DATA[j]['transliteration'])

    zig_zag = (lambda i: DATA[i]['transliteration'], lambda i: DATA[i]['transliteration'][::-1])

    return sorted_neighborhood(data, radius=1, window=20, distance=distance, keys=zig_zag)

# Applying clusterings
apply_clustering(clustering_0_exact, DATA)
apply_clustering(clustering_1_normalization, DATA)
apply_clustering(clustering_2_harsh_normalization, DATA)
apply_clustering(clustering_3_fingerprinting, DATA)
apply_clustering(clustering_4_bigram_fingerprinting, DATA, aggresive=True)
apply_clustering(clustering_5_cologne, DATA, aggresive=True)
apply_clustering(clustering_6_rusalka, DATA, aggresive=True)
apply_clustering(clustering_7_snm, DATA, aggresive=True)

ROWS_TO_MERGE = set()

for key, method in VALID_CLUSTERS.items():
    ROWS_TO_MERGE.update(key)

    for i in key:
        DATA[i]['valid_cluster'] = method
        print(DATA[i]['name'], DATA[i]['gender_B'], DATA[i]['birth_B'], DATA[i]['death_B'], DATA[i]['final_occupation_L2_B'], DATA[i]['final_citizenship'], DATA[i][method + '_confidence'], method)

    print()

print('Found a total of %i valid clusters gathering %i rows' % (len(VALID_CLUSTERS), len(ROWS_TO_MERGE)))

with open(OUTPUT, 'w') as of:
    writer = csv.DictWriter(of, fieldnames=fieldnames + FIELDNAMES_TO_ADD)
    writer.writeheader()

    for item in DATA:
        writer.writerow(item)
