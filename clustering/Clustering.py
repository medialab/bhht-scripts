
# coding: utf-8

# # Clustering of Wikipedia Names

# In[1]:


import re
import csv
import itertools
from collections import Counter
from unidecode import unidecode
from urllib.parse import unquote
from fog.clustering import key_collision, sorted_neighborhood
from fog.key import fingerprint, omission_key, skeleton_key
from fog.phonetics import cologne, rusalka
from fog.tokenizers import fingerprint_tokenizer, ngrams
from fog.phonetics.utils import squeeze
from Levenshtein import distance as levenshtein
from tqdm import tqdm


# ## Constants

# In[2]:


LANGS = {
    'english': 'en',
    'french': 'fr',
    'spanish': 'es',
    'portuguese': 'pt',
    'german': 'de',
    'italian': 'it',
    'swedish': 'sv'
}


# ## Helpers

# In[3]:


def is_cluster_relevant(cluster, results_index=None):
    """
    Function returning whether a cluster is relevant and the set of found ids.

    A cluster will be deemed relevant if:
        1) It contains names from more than one single id.
        2) If the range between birth/death does not exceed a given threshold.

    """
    ids = set()
    exact_birth_dates = set()

    for person in cluster:
        ids.add(person['id'])

        exact_birth_date = person['exact_birth']

        if exact_birth_date is not None:
            exact_birth_dates.add(exact_birth_date)

    if len(ids) == 1:
        return False, ids

    if len(exact_birth_dates) > 1:
        return False, ids

    if results_index is not None:
        result = results_index.get(tuple(sorted(ids)))

        if result is not None:
            return False, ids

    return True, ids


# In[4]:


def score_cluster(cluster):
    if len(set(p['birth'] for p in cluster if p['birth'] is not None)) == 1 and        len(set(p['death'] for p in cluster if p['death'] is not None)) == 1:
        return 'High'

    if any(p for p in cluster if p['exact_birth'] is None):
        return 'Low'

    return 'High'


# In[5]:


def print_cluster(cluster, ids):
    print('Found cluster containing %i ids and %i persons:' % (len(ids), len(cluster)))

    for person in cluster:
        print('  %s (%i) (%s - %s) (%s)' % (person['name'], person['id'], person['birth'], person['death'], person['lang']))

    print()


# In[6]:


def print_cluster_html(cluster, ids):

    confidence = score_cluster(cluster)

    print('<div>')
    print('  <p>')
    print('    (%s confidence) Found cluster containing %i ids and %i persons:' % (confidence, len(ids), len(cluster)))
    print('  </p>')

    print('  <ul>')
    for person in cluster:
        link = 'https://%s.wikipedia.org/wiki/%s' % (person['lang'], person['name'])
        print('    <li>%s (%i) (%s - %s) (%s) (%s) <u>(<a href="%s" target="_blank">link</a>)</u></li>' % (person['name'], person['id'], person['birth'], person['death'], person['exact_birth'], person['lang'], link))
    print('  </ul>')

    print('</div>')


# ## Processing data

# In[7]:


INPUT = './clustering.csv'
BIRTH_DATE_INPUT = './birthdates.csv'
OUTPUT = './found.csv'
BIRTH_DATE_INDEX = {}
PERSONS = []

with open(BIRTH_DATE_INPUT, 'r') as f:
    reader = csv.DictReader(f)

    for line in reader:
        b = line['birth_date']

        if b.endswith('00-00') or b.endswith('01-01'):
            continue

        BIRTH_DATE_INDEX[(line['lang'], line['name'])] = line['birth_date']

with open(INPUT, 'r') as f:
    reader = csv.DictReader(f)

    for line in tqdm(reader):
        for lang in LANGS:
            name = line['%s_link' % lang]

            if not name:
                continue

            person = {
                'id': int(line['id']),
                'lang': LANGS[lang],
                'name': name,
                'birth': int(line['birth_min']) if line['birth_min'] else None,
                'death': int(line['death_min']) if line['death_min'] else None,
                'exact_birth': BIRTH_DATE_INDEX.get((LANGS[lang], name)) or None
            }

            PERSONS.append(person)


# In[8]:


RESULTS_SO_FAR = './clustering_results.csv'
RESULTS_INDEX = {}
with open(RESULTS_SO_FAR, 'r') as f:
    reader = csv.DictReader(f)

    for line in reader:
        ids = tuple(sorted(int(i) for i in line['ids'].split('|')))

        RESULTS_INDEX[ids] = line['result']


# ## Cologne

# In[ ]:


# NUM_RE = re.compile(r'\d')

# def grouper(i):
#     x = PERSONS[i]

#     if not x['birth']:
#         return None

#     f = unquote(x['name'])
#     f = f.replace('_', ' ').replace('-', ' ')
#     f = squeeze(f)
#     f = f.split(' ')

#     f = [i for i in f if not re.match(NUM_RE, i)]

#     if len(f) == 0:
#         return None

#     try:
#         f = ' '.join(cologne(i) for i in f)
#     except:
#         return None

#     return (x['birth'], x['death'], f)

# of = open(OUTPUT, 'w')
# writer = csv.DictWriter(of, fieldnames=['lang', 'name'])
# writer.writeheader()

# clusters = key_collision(range(len(PERSONS)), key=grouper)

# RELEVANT_CLUSTERS = 0
# for cluster in clusters:
#     cluster = [PERSONS[i] for i in cluster]
#     relevant, ids = is_cluster_relevant(cluster, RESULTS_INDEX)

#     if not relevant:
#         continue

#     if not any(p for p in cluster if p['lang'] == 'de' or p['lang'] == 'sv'):
#         continue

#     if any(True for a, b in itertools.combinations([p['name'] for p in cluster], 2) if levenshtein(a, b) > 3):
#         continue

#     RELEVANT_CLUSTERS += 1

#     # print_cluster_html(cluster, ids)
#     # ids_str = '|'.join(set(str(p['id']) for p in cluster))
#     # print('accents,%s,oui' % ids_str)
#     for person in cluster:
#         writer.writerow({'lang': person['lang'], 'name': person['name']})

# print('Found %i relevant clusters' % RELEVANT_CLUSTERS)
# of.close()


# # ## Sorted Neighborhood

# # In[ ]:


# of = open(OUTPUT, 'w')
# writer = csv.DictWriter(of, fieldnames=['lang', 'name'])
# writer.writeheader()

# for p in PERSONS:
#     p['omission_key'] = omission_key(p['name'])

# distance = lambda a, b: levenshtein(PERSONS[a]['name'], PERSONS[b]['name'])

# def key(i):
#     p = PERSONS[i]

#     return (p['birth'] or 0, p['death'] or 0, p['omission_key'])

# clusters = list(sorted_neighborhood(range(len(PERSONS)), distance=distance, window=50, radius=1, key=key))

# RELEVANT_CLUSTERS = 0
# for cluster in clusters:
#     cluster = [PERSONS[i] for i in cluster]

#     births = set(p['birth'] for p in cluster)

#     if len(births) > 1:
#         continue

#     deaths = set(p['death'] for p in cluster if p['death'] is not None)

#     if len(deaths) > 1:
#         continue

#     relevant, ids = is_cluster_relevant(cluster, RESULTS_INDEX)

#     if not relevant:
#         continue

#     RELEVANT_CLUSTERS += 1

#     # print_cluster_html(cluster, ids)
#     # ids_str = '|'.join(set(str(p['id']) for p in cluster))
#     # print('accents,%s,oui' % ids_str)
#     for person in cluster:
#         writer.writerow({'lang': person['lang'], 'name': person['name']})

# print('Found %i relevant clusters' % RELEVANT_CLUSTERS)
# of.close()


# In[ ]:


of = open(OUTPUT, 'w')
writer = csv.DictWriter(of, fieldnames=['lang', 'name'])
writer.writeheader()

for p in PERSONS:
    p['skeleton_key'] = skeleton_key(p['name'])

distance = lambda a, b: levenshtein(PERSONS[a]['name'], PERSONS[b]['name'])

def key(i):
    p = PERSONS[i]

    return (p['birth'] or 0, p['death'] or 0, p['skeleton_key'])

clusters = list(sorted_neighborhood(range(len(PERSONS)), distance=distance, window=50, radius=2, key=key))

RELEVANT_CLUSTERS = 0
for cluster in clusters:
    cluster = [PERSONS[i] for i in cluster]

    births = set(p['birth'] for p in cluster)

    if len(births) > 1:
        continue

    deaths = set(p['death'] for p in cluster if p['death'] is not None)

    if len(deaths) > 1:
        continue

    relevant, ids = is_cluster_relevant(cluster, RESULTS_INDEX)

    if not relevant:
        continue

    RELEVANT_CLUSTERS += 1

    print_cluster_html(cluster, ids)
    # ids_str = '|'.join(set(str(p['id']) for p in cluster))
    # print('accents,%s,oui' % ids_str)
    for person in cluster:
        writer.writerow({'lang': person['lang'], 'name': person['name']})

print('Found %i relevant clusters' % RELEVANT_CLUSTERS)
of.close()

