# =========================
# BHHT Mining Configuration
# =========================
#
DEBUG = False

PROXY = None

DATA = {
    'people': '../../bhht-datascape/scripts/data/base1_individus.csv',
    'location': '../../bhht-datascape/scripts/data/base2_locations_with_lang.csv'
}

MONGODB = {
    'host': 'localhost',
    'port': 27017
}

PROCESSORS = 6
