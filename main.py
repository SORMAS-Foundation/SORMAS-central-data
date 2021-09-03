import csv
import json
import uuid
from os import path

# 1. download https://confluence.sormas-tools.de/download/attachments/14492867/sormas_infrastrukturdaten_master.zip?version=3&modificationDate=1625495830588&api=v2
# 2. unzip to to 'import/csv/' RELATIVE to main.py
# 3. create folder named 'out'
# 4. run this script

csv_files = {
    'countries': 'sormas_laender_survnet.csv',
    'regions': 'sormas_bundeslaender_master.csv',
    'districts': 'sormas_landkreise_master.csv',
    'communities': 'sormas_gemeinden_master.csv'
}


def get_external_id(line: dict):
    if 'externalId' in line.keys():
        return line['externalId']
    else:
        return line['externalID']


def get_name(line: dict):
    if 'name' in line.keys():
        return line['name']
    else:
        return line['defaultName']


def main():
    for item in csv_files.keys():
        with open(path.join('import', 'csv', csv_files[item]), 'r') as f:
            converted = list()
            reader = csv.DictReader(f, delimiter=';')
            for line in reader:
                # don't mind the URL prefix, we just need something which is fixed
                line['uuid'] = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                              f"https://de.central.sormas-oegd.de/{get_external_id(line)}/{get_name(line)}"))
                converted.append({'key': line['uuid'], 'value': line})
            with open(path.join('out', f'{item}.json'), 'w+', encoding='utf8') as out:
                json.dump(converted, out, indent=2, ensure_ascii=False)


if __name__ == '__main__':
    main()
