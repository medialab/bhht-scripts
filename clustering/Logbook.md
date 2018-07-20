# Clustering Log Book

## 1. Connected components desambiguation

* Availables features are not enough. Need to retrieve exact birth date from Wikidata to be able to trim out irrelevant clusters.
* Fixed clusters related to desambiguation pages.
* Confidence score based on features similarity + wikidata exact birth date.
* Found 493 clusters being past or present missing links and case normalization.

## 2. Normalization

1. Normalizing hyphens to underscores.
2. Normalizing url components, dropping non alphanum characters.
3. Normalizing accents and unicode letters (ß).
4. Fingerprinting.
5. Squeezed fingerprinting.
6. Dropping small tokens.
7. Rusalka (phonetic).

Doublons internes à une langue.
