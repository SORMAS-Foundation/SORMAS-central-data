import csv
import json
import uuid


def read_csv(path, delimiter=','):
    with open(path, 'r') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        fieldnames = list(reader.fieldnames)
        fieldnames.append('uuid')
        result = list()
        for line in reader:
            keys = sorted(line.keys())
            joined_values = ''.join([line[k] for k in keys])
            _uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f'https://sormas.org/location/{joined_values}'))
            line['uuid'] = _uuid
            result.append({'key': _uuid, 'value': line})
        return result, fieldnames


def write_csv(out, path):
    out, fieldnames = out
    with open(path, 'w+', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for line in out:
            writer.writerow(line['value'])


def write_json(out, path):
    out, fieldnames = out
    with open(path, 'w+', encoding='utf8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)


def store(out, path):
    write_json(out, path + '.json')
    write_csv(out, path + '.csv')


def main():
    print("International data processing")
    int_continents = read_csv('./import/csv/international/sormas_import_all_continents.csv', ',')
    store(int_continents, './out/international/sormas_import_all_continents')
    int_subcontinents = read_csv('./import/csv/international/sormas_import_all_subcontinents.csv', ',')
    store(int_subcontinents, './out/international/sormas_import_all_subcontinents')
    int_countries = read_csv('./import/csv/international/sormas_import_all_countries.csv', ',')
    store(int_countries, './out/international/sormas_import_all_countries')

    print("Germany")
    int_countries = read_csv('./import/csv/germany/sormas_laender_survnet.csv', ';')
    store(int_countries, './out/germany/countries')

    int_regions = read_csv('./import/csv/germany/sormas_bundeslaender_master.csv', ';')
    store(int_regions, './out/germany/regions')

    int_districts = read_csv('./import/csv/germany/sormas_landkreise_master.csv', ';')
    store(int_districts, './out/germany/districts')

    int_communities = read_csv('./import/csv/germany/sormas_gemeinden_master.csv', ';')
    store(int_communities, './out/germany/communities')


if __name__ == '__main__':
    main()
