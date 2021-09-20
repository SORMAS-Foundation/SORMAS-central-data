import csv
import json
import uuid

continent_dtos = None
subcontinent_dtos = None
country_dtos = None

region_dtos = None
district_dtos = None


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
            result.append(line)
        return result, fieldnames


def write_csv(out, path):
    out, fieldnames = out
    with open(path, 'w+', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for line in out:
            writer.writerow(line)


needs_ref_dto = {'region', 'district', 'country', 'subcontinent', 'continent'}


def make_ref_dtos(out, key, dtos, filter_expr='name'):
    for needs_lookup in out:
        entity_name = needs_lookup[key]
        del needs_lookup[key]

        if entity_name == 'Germany':
            entity_name = 'Deutschland'

        candidate = list(filter(lambda f: f[filter_expr] == entity_name, dtos))
        assert len(candidate) == 1
        uuid = candidate[0]['uuid']
        needs_lookup[key] = {'uuid': uuid}
    return out


def insert_ref_dtos(out):
    dto_keys = set(out[0].keys())
    ref_keys = dto_keys.intersection(needs_ref_dto)
    if not ref_keys:
        return out

    if 'continent' in ref_keys:
        out = make_ref_dtos(out, 'continent', continent_dtos, filter_expr='defaultName')
    if 'subcontinent' in ref_keys:
        out = make_ref_dtos(out, 'subcontinent', subcontinent_dtos, filter_expr='defaultName')
    if 'country' in ref_keys:
        out = make_ref_dtos(out, 'country', country_dtos, filter_expr='defaultName')
    if 'region' in ref_keys:
        out = make_ref_dtos(out, 'region', region_dtos)
    if 'district' in ref_keys:
        out = make_ref_dtos(out, 'district', district_dtos)

    return out


def write_json(entities, path):
    entities, fieldnames = entities
    processed = insert_ref_dtos(entities)

    out = list()
    for p in processed:
        out.append({'key': p['uuid'], 'value': p})

    with open(path, 'w+', encoding='utf8') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)


def store(out, path):
    write_csv(out, path + '.csv')
    write_json(out, path + '.json')


def main():
    print("International data processing")

    int_continents = read_csv('./import/csv/international/sormas_import_all_continents.csv', ',')
    global continent_dtos
    continent_dtos = int_continents[0]
    store(int_continents, './out/international/sormas_import_all_continents')

    int_subcontinents = read_csv('./import/csv/international/sormas_import_all_subcontinents.csv', ',')
    global subcontinent_dtos
    subcontinent_dtos = int_subcontinents[0]
    store(int_subcontinents, './out/international/sormas_import_all_subcontinents')

    int_countries = read_csv('./import/csv/international/sormas_import_all_countries.csv', ',')
    store(int_countries, './out/international/sormas_import_all_countries')

    print("Germany")
    int_countries = read_csv('./import/csv/germany/sormas_laender_survnet.csv', ';')
    global country_dtos
    country_dtos = int_countries[0]
    store(int_countries, './out/germany/countries')

    int_regions = read_csv('./import/csv/germany/sormas_bundeslaender_master.csv', ';')
    global region_dtos
    region_dtos = int_regions[0]
    store(int_regions, './out/germany/regions')

    int_districts = read_csv('./import/csv/germany/sormas_landkreise_master.csv', ';')
    global district_dtos
    district_dtos = int_districts[0]
    store(int_districts, './out/germany/districts')

    int_communities = read_csv('./import/csv/germany/sormas_gemeinden_master.csv', ';')
    store(int_communities, './out/germany/communities')


if __name__ == '__main__':
    main()
