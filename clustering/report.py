import csv
import itertools
import networkx as nx
from collections import defaultdict, Counter, OrderedDict
from tqdm import tqdm

DATA = './base1_individus.csv'
CLUSTERING_RESULT = './fixed_clustering_results.csv'

PERSONS = {}
CLUSTERS_IDS = defaultdict(list)
RESULTS_COUNTER = Counter()
IDS_COUNTER = Counter()
SETS_COUNTER = Counter()
FALSE_POSITIVES = Counter()
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
        PERSONS['%s:%s' % (line['id'], line['language'])] = line
        IDS_COUNTER[line['id']] += 1

with open(CLUSTERING_RESULT, 'r') as f:
    reader = csv.DictReader(f)

    for line in reader:
        key = tuple(sorted(line['ids'].split('|')))
        result = line['result']

        CLUSTERS_IDS[key].append(line['set'])
        RESULTS_COUNTER[result] += 1

        if result == 'non':
            FALSE_POSITIVES[line['set']] += 1

        if result != 'oui':
            continue

        SETS_COUNTER[line['set']] += 1

        for a, b in itertools.combinations(line['ids'].split('|'), 2):
            GRAPH.add_node(a, set=line['set'])
            GRAPH.add_node(b, set=line['set'])
            GRAPH.add_edge(a, b)

TRUE_CLUSTERS = list(nx.connected_components(GRAPH))

# Overriding with actual data
FALSE_POSITIVES['cologne'] = 485 # 11655
FALSE_POSITIVES['snm_skeleton_lev1'] = 468
# lev2 7265 -> 3 vrais positifs -> 369 sample
# 33739511|6939994,5877465|48798052,25274649|3057649

TITLE = '1) Sanity tests:'
print(TITLE)
print('-' * len(TITLE))
if any(True for results in CLUSTERS_IDS.values() if len(results) > 1):
    print('- [WARNING]: Found duplicate duplicates.')
    print()

    for ids, results in CLUSTERS_IDS.items():
        if len(results) > 1:
            print(ids)
            print(len(results))
else:
    print('✓ No duplicate duplicates.')

# for cluster in TRUE_CLUSTERS:
#     if any(True for unique_id in cluster if IDS_COUNTER[unique_id.split(':', 1)[0]] > 1):
#         print('[WARNING]: Found ambiguous cluster:')

#         for unique_id in cluster:
#             print('  -', PERSONS[unique_id]['name'], unique_id)

#         print()

print()

TITLE = '2) Statistics:'
print(TITLE)
print('-' * len(TITLE))
print()

print('Results')
print('  - Total: %i' % len(list(RESULTS_COUNTER.elements())))
for result, count in RESULTS_COUNTER.most_common():
    print('  - %s: %i' % (result, count))

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
    'high_confidence_cc': 'High Confidence Connected Components',
    'low_confidence_cc': 'Low Confidence Connected Components',
    'normalization': 'Aggressive Normalization',
    'unicode': 'Unicode standardization',
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
    false_positives = FALSE_POSITIVES[slug]
    true_positives = SETS_COUNTER[slug]
    N = false_positives + true_positives
    precision = true_positives / N

    print('  %i. %s (%s) => %i/%i (precision: %f)' % (i + 1, label, slug, true_positives, N, precision))

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
                    print('  - (%s) %s %s | %s' % (lang, unique_id.ljust(20, ' '), name, link))

        print()

    print()

print()
