import psycopg
import json
from psycopg.rows import dict_row

import logging

logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

CONNECTION = "host= localhost dbname=sormas_db user=postgres password=postgres"


def iterate_central():
    infra_types = {
        'continent': 'out/international/sormas_import_all_continents.json',
        'subcontinent': 'out/international/sormas_import_all_subcontinents.json',
        'country': 'out/germany/countries.json',
        'region': 'out/germany/regions.json',
        'district': 'out/germany/districts.json',
        'community': 'out/germany/communities.json'
    }

    for table, path in infra_types.items():
        logging.info(f"Process table {table}")
        with open(path) as f:
            entities = json.load(f)

        for entity in entities:
            central_value = entity['value']
            # 1. Check for UUID match
            if check_local_uuid(table, central_value):
                logging.info(f"Exact local match! Skipping {table}: {central_value['uuid']}")
                continue
            if check_local_name_and_id(table, central_value):
                continue
            # insert value


def check_local_name_and_id(table, central_value):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            where_name = central_value['defaultName'] if has_default_name(table) else {central_value['name']}
            external_id = central_value['externalId']
            where_ext_id = f"externalid={external_id}"
            if has_default_name(table):
                local = cur.execute(f"SELECT * FROM {table} WHERE defaultname=%s OR externalid=%s;",
                                    (where_name, where_ext_id)).fetchone()
            else:
                local = cur.execute(f"SELECT * FROM {table} WHERE name=%s OR externalid=%s;",
                                    (where_name, where_ext_id)).fetchone()
            if local:
                # we found a local match which is also present in central, but with a different UUID
                uuid_ = central_value['uuid']
                # update local UUID
                if has_default_name(table):
                    cur.execute(f"UPDATE {table} SET uuid=%s WHERE defaultname=%s OR externalid=%s;",
                                (uuid_, where_name, where_ext_id))
                else:
                    cur.execute(f"UPDATE {table} SET uuid=%s WHERE name=%s OR externalid=%s;",
                                (uuid_, where_name, where_ext_id))
                    logging.info(f"Updated {table} from {local['uuid']} to {uuid_}")
            else:
                logging.error(
                    f"Could not find local item in {table} with external ID {external_id} and {where_name}")


def check_local_uuid(table, central_value):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            local = cur.execute(f"SELECT * FROM {table} WHERE uuid=%s", [central_value['uuid']]).fetchone()
            if local:
                ext_id = central_value['externalId'] == local['externalid']
                central_name = central_value[
                    'defaultName' if has_default_name(table) else 'name']
                local_name = local[
                    'defaultname' if has_default_name(table) else 'name']
                name = central_name == local_name
                sanity = ext_id and name
                if not sanity:
                    logging.error("Foo")
                return sanity
            else:
                return False


def has_default_name(table):
    return table in ['continent', 'subcontinent', 'country']


def iterate_local():
    pass


# Connect to an existing database


def main():
    iterate_central()
    logging.info("All done")


if __name__ == '__main__':
    main()
