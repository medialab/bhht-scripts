# BHHT Mining Scripts

Collection of python scripts used to collect & mine BHHT's wikipedia data.

## Installation

Requires python >=3.6.

To install dependencies (in a virtual environment preferably), you can use pip:

```
pip install -r requirements.txt
```

You will also need a MongoDB running on the server. All the scripts will write in a database named `bhht`.

This database will hold three collections: `people`, `location` & `entities`.

## Configuration

You first need to copy the default configuration.

```
cp config.default.py config.py
```

To edit scrapy's settings if needed (mostly to adjust rates):

```
vim mining/settings.py
```

## Scripts

### MongoDB processing queue creation

First, you will need to create processing queues for people, location and, later, wikidata entities.

```
# For people pages
./create_people_queue.py

# For location pages
./create_location_queue.py

# For entity pages
./create_entities_queue.py
```

### Scrapy spiders

You can find everything related to scrapy in `mining`.

There are 4 spiders:

1. `entities`: retrieves information about wikidata entities (`Q245`, for instance).
2. `location`: retrieves the HTML of the location pages.
3. `people`: retrieves the HTML of the people pages.
4. `wikidata`: retrieves and parse wikidata information about the location pages.

To run a spider:

```
scrapy crawl [name-of-spider]
# Example:
scrapy crawl location
```

### Dumps

```
# Dumping the "errors" (404 & 400) for a model
./dump_errors.py [people-or-location] [output-folder]

# Dumping the links
./dump_links.py [output-folder]

# Dumping the location sets in binary format (needed by link extraction)
./dump_location_sets.py [output-folder]
```

### Extraction

To run the link extraction (you will need to compute the binary location sets before):

```
./extract_links.py [locations-sets-path]
```
