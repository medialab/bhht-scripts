import csv
import itertools
import networkx as nx
from collections import defaultdict, Counter, OrderedDict
from tqdm import tqdm

DATA = './clustering.csv'
CLUSTERING_RESULT = './clustering_results.csv'

PERSONS = {}
CLUSTERS_IDS = defaultdict(list)
RESULTS_COUNTER = Counter()
SETS_COUNTER = Counter()
GRAPH = nx.Graph()

LANGS = {
    'english': 'en',
    'french': 'fr',
    'spanish': 'es',
    'portuguese': 'pt',
    'german': 'de',
    'italian': 'it',
    'swedish': 'sv'
}

print()
TITLE = 'Clustering report'
print(TITLE)
print('=' * len(TITLE))
print()

with open(DATA, 'r') as f:
    reader = csv.DictReader(f)

    for line in tqdm(reader, 'Reading CSV data', total=1846130):
        PERSONS[line['id']] = line

with open(CLUSTERING_RESULT, 'r') as f:
    reader = csv.DictReader(f)

    for line in reader:
        key = tuple(sorted(line['ids'].split('|')))
        result = line['result']

        CLUSTERS_IDS[key].append(line['set'])
        RESULTS_COUNTER[result] += 1

        if result != 'oui':
            continue

        SETS_COUNTER[line['set']] += 1

        for a, b in itertools.combinations(line['ids'].split('|'), 2):
            GRAPH.add_node(a, set=line['set'])
            GRAPH.add_node(b, set=line['set'])
            GRAPH.add_edge(a, b)

TITLE = '1) Sanity tests:'
print(TITLE)
print('-' * len(TITLE))
if any(True for results in CLUSTERS_IDS.values() if len(results) > 1):
    print('- [WARNING]: Found duplicate duplicates.')
else:
    print('✓ No duplicate duplicates.')

print()

TITLE = '2) Statistics:'
print(TITLE)
print('-' * len(TITLE))
print()

print('Results')
print('  - Total: %i' % len(list(RESULTS_COUNTER.elements())))
for result, count in RESULTS_COUNTER.most_common():
    print('  - %s: %i' % (result, count))

TRUE_CLUSTERS = list(nx.connected_components(GRAPH))
CLUSTERS_SIZE_DISTRIBUTION = Counter()

for cluster in TRUE_CLUSTERS:
    CLUSTERS_SIZE_DISTRIBUTION[len(cluster)] += 1

print()
print('Found %i duplicate components:' % len(TRUE_CLUSTERS))

for i in range(2, 4):
    print('  - %i %s-components.' % (CLUSTERS_SIZE_DISTRIBUTION[i], i))

# print()
# print('Language stats:')

# for cluster in TRUE_CLUSTERS:

#     langs = Counter()

#     for unique_id in cluster:
#         p = PERSONS[unique_id]

#         for label, lang in LANGS.items():
#             if p['%s_link' % label]:
#                 langs[lang] += 1

#     intersections = [lang for lang, count in langs.items() if count > 1]

#     print(intersections)


# NOTE: if no intersection, we must find the number of items that will be
# dropped per langs & number of lines that will be dropped

print()

TITLE = '3) Clustering sets:'
print(TITLE)
print('-' * len(TITLE))
print()

SETS = OrderedDict({
    'low_confidence_cc': 'Low Confidence Connected Components',
    'high_confidence_cc': 'High Confidence Connected Components',
    'normalization': 'Aggressive Normalization',
    'unicode': 'Unidode standardization',
    'fingerprint': 'String Fingerprint',
    'squeezed_fingerprint': 'Squeezed String Fingerprint' ,
    'small_tokens': 'Small Tokens Dropping',
    'rusalka': 'Rusalka Phonetic Encoding',
    'snm_omission_lev1': 'Sorted Neighborhood Omission Key Levenshtein-1',
    'snm_skeleton_lev1': 'Sorted Neighborhood Skeleton Key Levenshtein-1',
    'cologne': 'Kölner Phonetic Encoding'
})

print('Summary')

for i, (slug, label) in enumerate(SETS.items()):
    print('  %i. %s (%s) => %i' % (i + 1, label, slug, SETS_COUNTER[slug]))

print()

for i, (slug, label) in enumerate(SETS.items()):
    subtitle = '%i. %s (%s) => %i' % (i + 1, label, slug, SETS_COUNTER[slug])
    print(subtitle)
    print('*' * len(subtitle))
    print()

    clusters = [c for c in TRUE_CLUSTERS if GRAPH.nodes[list(c)[0]]['set'] == slug]

    for cluster in clusters:
        for unique_id in sorted(cluster):
            p = PERSONS[unique_id]

            for label, lang in LANGS.items():
                name = p['%s_link' % label]
                if name:
                    link = 'https://%s.wikipedia.org/wiki/%s' % (lang, name)
                    print('  - (%s) %s %s | %s' % (lang, unique_id.ljust(8, ' '), name, link))

        print()

    print()

print()
