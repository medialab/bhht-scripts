import csv
from collections import defaultdict
from heapq import nsmallest

CLUSTERS = defaultdict(list)

with open('./tocheck.csv') as f:
    for line in csv.DictReader(f):
        cluster_method = line['valid_cluster']
        cluster_id = line[cluster_method]

        CLUSTERS[(cluster_method, cluster_id)].append(line)

def get_ranking(row):
    ranking = row['ranking_final_B_5']

    if not ranking:
        return 10_000_000.0

    return float(ranking)

def cluster_key(cluster):
    return min(get_ranking(row) for row in cluster)

top = nsmallest(50, CLUSTERS.values(), key=cluster_key)


for cluster in top:

    print()
    for row in sorted(cluster, key=get_ranking):
        print(
            row['wikidata_code'],
            row['name'],
            row['ranking_final_B_5'],
            row['valid_cluster'],
            row['%s_confidence' % row['valid_cluster']]
        )
