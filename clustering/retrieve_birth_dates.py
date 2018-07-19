import csv
import requests
import sys

INPUT = sys.argv[1]
OUTPUT = 'birthdates.csv'

BASE_URL = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=claims'


def create_url(lang, name):
    return '%s&sites=%swiki&titles=%s' % (BASE_URL, lang, name)


with open(INPUT, 'r') as input_file, \
     open(OUTPUT, 'a') as outputfile:
     reader = csv.DictReader(input_file)
     writer = csv.DictWriter(outputfile, fieldnames=['name', 'lang', 'birth_date'])
     writer.writeheader()

     for line in reader:
        url = create_url(line['lang'], line['name'])
        print(url)

        r = requests.get(url)

        data = r.json()

        # Finding the correct claim
        claims = next(iter(data['entities'].values())).get('claims')
        birthdate = None

        if claims is not None:

            birthdate_claim = next((v for k, v in claims.items() if k == 'P569'), None)

            if birthdate_claim is not None:
                birthdate = birthdate_claim[0]['mainsnak']['datavalue']['value']['time'].lstrip('+').split('T', 1)[0]

        writer.writerow({
            'lang': line['lang'],
            'name': line['name'],
            'birth_date': birthdate if birthdate is not None else ''
        })
