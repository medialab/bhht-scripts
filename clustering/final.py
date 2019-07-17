import re
import csv
import codecs
import itertools
from tqdm import tqdm
from collections import defaultdict
from unidecode import unidecode
from fog.clustering import passjoin, key_collision, sorted_neighborhood
from fog.metrics import overlap_coefficient
from fog.phonetics import cologne, rusalka
from fog.utils import squeeze
from fog.key import fingerprint, ngrams_fingerprint
from statistics import mean, median, stdev
from Levenshtein import distance as levenshtein

INPUT = './final-with-ranking.csv'
WIKIDATA_EXTERNAL_SOURCES = './wikidata_external_sources.csv'
OUTPUT = './final-clustering.csv'

TEST_RUN = False
TEST_RUN_BATCH = 1_000_000

FIELDNAMES_TO_ADD = [
    'transliteration',
    'normalized_transliteration',
    'valid_cluster'
]

NUMBER_RE = re.compile(r'(?:^[.,XVI0-9\-]+$|[()])', re.I)
NON_LATIN_RE = re.compile(r'[^0-9A-Za-z\u00C0-\u00D6\u00D8-\u00f6\u00f8-\u00ff\s]', re.I)

def process(name):
    return name.lower().strip()

def process_harsher(name):
    return squeeze(process(name), keep_roman_numerals=True).replace('-', '').replace('_', '')

def safe_cologne(name):
    try:
        return cologne(name)
    except:
        return None

def has_non_latin_characters(name):
    return bool(NON_LATIN_RE.search(name))

def tokenize(name):
    name = name.replace('-', '_').lower()
    return name.split('_')

def initialize(name):
    tokens = tokenize(name)

    if any(NUMBER_RE.match(t) for t in tokens):
        return name

    if len(tokens) < 2:
        return name

    try:
        initials = '_'.join(s[0] + '.' for s in tokens[:-1])
    except:
        return name

    return initials + tokens[-1]

def missing(value):
    return not value or value == 'Other' or value == '.' or value == 'missing'

CONFIDENCE_TOTAL = 4.0
def confidence_score(cluster, boosted=False):
    score = 0

    genders = set(r['gender_B'] for r in cluster)
    genders_without_missing = set(r['gender_B'] for r in cluster if not missing(r['gender_B']))

    births = set(r['birth_B'] for r in cluster)
    births_without_missing = set(r['birth_B'] for r in cluster if not missing(r['birth_B']))

    deaths = set(r['death_B'] for r in cluster)
    deaths_without_missing = set(r['death_B'] for r in cluster if not missing(r['death_B']))

    occupations = set(r['final_occupation_L2_B'] for r in cluster)
    occupations_without_missing = set(r['final_occupation_L2_B'] for r in cluster if not missing(r['final_occupation_L2_B']))

    citizenships = set(r['final_citizenship'] for r in cluster)
    citizenships_without_missing = set(r['final_citizenship'] for r in cluster if not missing(r['final_citizenship']))

    if (
        len(genders_without_missing) > 1 or
        len(births_without_missing) > 1 or
        len(deaths_without_missing) > 1
    ):
        return 0

    score += 1 - len([r['gender_B'] for r in cluster if missing(r['gender_B'])]) / len(cluster)
    score += 1 - len([r['birth_B'] for r in cluster if missing(r['birth_B'])]) / len(cluster)
    score += 1 - len([r['death_B'] for r in cluster if missing(r['death_B'])]) / len(cluster)

    if boosted and score == 0:
        score = 0.5

    if len(occupations_without_missing) < 2:
        score += 0.5 - len([r['final_occupation_L2_B'] for r in cluster if missing(r['final_occupation_L2_B'])]) / len(cluster) / 2.0

    if len(citizenships_without_missing) < 2:
        score += 0.5 - len([r['final_citizenship'] for r in cluster if missing(r['final_citizenship'])]) / len(cluster) / 2.0

    normalized = score / CONFIDENCE_TOTAL

    if (
        normalized == 0.75 and
        not any(missing(r['birth_B']) for r in cluster) and
        len(occupations) < 2 and
        len(citizenships) < 2
    ):
        normalized += 0.05

    return normalized

EXTERNAL = defaultdict(list)
DATA = []

TUPLE_RE = re.compile(r"""(?<=['"]\)),\s""")

def parse_tuples(string):

    if not string.strip():
        return

    for entry in TUPLE_RE.split(string):
        t = eval(entry)
        yield t[0], t[2]

# Aggregating external sources
with open(WIKIDATA_EXTERNAL_SOURCES) as f:
    reader = csv.reader(f)
    next(reader)

    if TEST_RUN:
        reader = itertools.islice(reader, 0, TEST_RUN_BATCH)

    for row in tqdm(reader, desc='Reading external sources'):
        wikidata_code = row[0]

        for key, identifier in parse_tuples(row[2]):
            EXTERNAL[wikidata_code].append((key, identifier))

# Reading data
with codecs.open(INPUT, encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    buckets = defaultdict(list)

    if TEST_RUN:
        reader = itertools.islice(reader, 0, TEST_RUN_BATCH)

    for line in tqdm(reader, desc='Processing data'):
        line['normalized_transliteration'] = unidecode(line['name'])

        if has_non_latin_characters(line['name']):
            line['transliteration'] = line['normalized_transliteration']
        else:
            line['transliteration'] = line['name']

        DATA.append(line)

VALID_CLUSTERS = {}

def apply_clustering(method, data, aggresive=False, unambiguous=False):
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

        if unambiguous:
            confidence = 1
        else:
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

    invalid_C = [c for c in C if c == 0]
    valid_C = [c for c in C if c >= threshold]

    category = 'aggresive' if aggresive else 'mild'

    if unambiguous:
        category = 'unambiguous'

    print('[%s] %s' % (name, category))
    print('  clusters: %i' % n)
    print('  0 clusters: %i (%2f)' % (len(invalid_C), len(invalid_C) / n))
    print('  >0 clusters: %i (%2f)' % (len(non_zero_C), len(non_zero_C) / n))
    print('  >%s clusters: %i (%2f)' % (str(threshold), len(valid_C), len(valid_C) / n))
    print('  >%s never seen before clusters: %i (%2f)' % (str(threshold), V, V / n))
    print('  rows: %i *%i' % (I, len(non_zero_C)))
    print('  confidence avg: %2f *%2f' % (mean(C), non_zero_mean))
    print('  confidence median: %2f *%2f' % (median(C), non_zero_median))
    print()

# 0a
def clustering_0a_external_identifiers(data):

    default_list = []

    def keys(i):
        code = DATA[i].get('wikidata_code')

        if code is None:
            return

        return EXTERNAL.get(code, default_list)

    for cluster in key_collision(data, keys=keys, max_size=2):

        # TODO: remove when fog is fixed
        if len(cluster) > 2:
            continue

        codeA = DATA[cluster[0]].get('wikidata_code')
        codeB = DATA[cluster[1]].get('wikidata_code')

        # We want perfect overlap
        A = EXTERNAL.get(codeA, set()) if codeA else set()
        B = EXTERNAL.get(codeB, set()) if codeB else set()

        # NOTE: we could rely on key intersection match
        # if overlap_coefficient(A, B) != 1:
        #     continue

        Ak = set(s for s, _ in A)
        Bk = set(s for s, _ in B)

        Ik = Ak & Bk

        assert len(Ik) > 0

        Ai = set(p for p in A if p[0] in Ik)
        Bi = set(p for p in B if p[0] in Ik)

        # print()
        if Ai != Bi:
        #     print('DISCARD')
        #     print(A)
        #     print(B)
        #     print(Ik)
        #     print(DATA[cluster[0]]['name'], '=>', DATA[cluster[1]]['name'])
            continue

        # print('KEEP')
        # print(A)
        # print(B)
        # print(Ik)
        # print(DATA[cluster[0]]['name'], '=>', DATA[cluster[1]]['name'])

        yield cluster


# 0b Exact
def clustering_0b_exact(data):
    return key_collision(data, key=lambda i: DATA[i]['name'])

# 1. Basic normalization
def clustering_1_normalization(data):
    return key_collision(data, key=lambda i: process(DATA[i]['transliteration']))

# 2. Harsher normalization
def clustering_2_harsh_normalization(data):
    return key_collision(data, key=lambda i: process_harsher(DATA[i]['transliteration']))

# 3. Initials normalization
def clustering_3_initials(data):
    for cluster in key_collision(data, key=lambda i: initialize(DATA[i]['transliteration']), max_size=2):

        # We check the cluster once more:
        # If no item in the cluster has initials, we filter it
        # NOTE: it deprives us of some clusters where Aleks would match Aleksander
        # if not any(any(len(token.replace('.', '')) < 2 for token in tokenize(DATA[i]['name'])[:-1]) for i in cluster):
        #     pass

        yield cluster

# 4. Fingerprinting
def clustering_4_fingerprinting(data):
    return key_collision(data, key=lambda i: fingerprint(DATA[i]['transliteration']))

# 5. Bigram fingerprinting
def clustering_5_bigram_fingerprinting(data):
    return key_collision(data, key=lambda i: ngrams_fingerprint(2, DATA[i]['transliteration']))

# 6. Cologne
def clustering_6_cologne(data):
    return key_collision(data, key=lambda i: safe_cologne(DATA[i]['transliteration']))

# 7. Rusalka
def clustering_7_rusalka(data):
    return key_collision(data, key=lambda i: rusalka(DATA[i]['transliteration']))

# 8. SNM k=1
def clustering_8_snm(data):

    distance = lambda i, j: levenshtein(DATA[i]['transliteration'], DATA[j]['transliteration'])

    zig_zag = (lambda i: DATA[i]['transliteration'], lambda i: DATA[i]['transliteration'][::-1])

    return sorted_neighborhood(data, radius=1, window=20, distance=distance, keys=zig_zag)

# Applying clusterings
apply_clustering(clustering_0a_external_identifiers, DATA, unambiguous=True)
apply_clustering(clustering_0b_exact, DATA)
apply_clustering(clustering_1_normalization, DATA)
apply_clustering(clustering_2_harsh_normalization, DATA)
apply_clustering(clustering_3_initials, DATA, aggresive=True)
apply_clustering(clustering_4_fingerprinting, DATA, aggresive=True)
apply_clustering(clustering_5_bigram_fingerprinting, DATA, aggresive=True)
apply_clustering(clustering_6_cologne, DATA, aggresive=True)
apply_clustering(clustering_7_rusalka, DATA, aggresive=True)
apply_clustering(clustering_8_snm, DATA, aggresive=True)

ROWS_TO_MERGE = set()
RANKINGS = []

for key, method in VALID_CLUSTERS.items():
    ROWS_TO_MERGE.update(key)

    for i in key:
        DATA[i]['valid_cluster'] = method
        ranking = DATA[i]['ranking_final_B_5']

        if ranking:
            ranking = float(ranking)
            RANKINGS.append(ranking)
        # print(DATA[i]['name'], DATA[i]['gender_B'], DATA[i]['birth_B'], DATA[i]['death_B'], DATA[i]['final_occupation_L2_B'], DATA[i]['final_citizenship'], DATA[i][method + '_confidence'], method)

    # print()

print('Found a total of %i valid clusters gathering %i rows' % (len(VALID_CLUSTERS), len(ROWS_TO_MERGE)))
print('  Median ranking: %2f' % median(RANKINGS))
print('  Mean ranking: %2f' % mean(RANKINGS))
print('  Stdev ranking: %2f' % stdev(RANKINGS))
print('  Min ranking: %2f' % min(RANKINGS))

with open(OUTPUT, 'w') as of:
    writer = csv.DictWriter(of, fieldnames=fieldnames + FIELDNAMES_TO_ADD)
    writer.writeheader()

    for item in DATA:
        writer.writerow(item)
