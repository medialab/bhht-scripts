# BHHT Deduplication Routine

To run the deduplication routine on the bhht dataset, one needs to have:

1. A file named `final-with-ranking.csv` in this directory, containing the unique records per notable people, along with their current ranking.
2. A file named `wikidata_external_sources.csv` containing the association between names and wikidata identifiers, which is useful to deduplicate by leveraging connected components of aliases in Wikidata.
3. To run the `final.py` script.
4. This will result in an enhanced file named `final-clustering.csv` containing new columns for transliterated names (used when running fuzzy clustering methods, but that can be useful downstream), as well as a column indicating in which non-singleton cluster of duplicates a record was found (after applying some relevancy filters).
