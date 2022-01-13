import os

import psycopg
import json
import argparse

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row

import logging

error_list = []

FORMAT = '%(message)s'

logging.basicConfig(encoding='utf-8', level=logging.DEBUG, format=FORMAT)

parser = argparse.ArgumentParser()
parser.add_argument("-H", "--host", default=os.environ.get("host"), help="database server host or socket directory",
                    action="store")
parser.add_argument("-d", "--dbname", default=os.environ.get("dbname"), help="database name to connect to)",
                    action="store")
parser.add_argument("-u", "--username", default=os.environ.get("username"),
                    help="database user name",
                    action="store")
parser.add_argument("-p", "--password", default=os.environ.get("password"), help="password for user", action="store")
parser.add_argument("-i", "--input", default=os.environ.get("input"), help="path where to expect the central data", action="store")
args, unknown = parser.parse_known_args()

assert len(unknown) == 0

CONNECTION = f"host={args.host} dbname={args.dbname} user={args.username} password={args.password}"
logging.info(f'Connecting to {args.host}')
PATH = args.input


def archive_everything(table):
    logging.info(f"Archive {table}")
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("UPDATE featureconfiguration SET enabled=FALSE WHERE featuretype='EDIT_INFRASTRUCTURE_DATA';")
            cur.execute(f"UPDATE {table} SET archived=TRUE WHERE TRUE;")


def iterate_central():
    infra_types = {
        'continent': f'{PATH}/international/continent.json',
        'subcontinent': f'{PATH}/international/subcontinent.json',
        'country': f'{PATH}/germany/country.json',
        'region': f'{PATH}/germany/region.json',
        'district': f'{PATH}/germany/district.json',
        'community': f'{PATH}/germany/community.json'
    }

    for table, path in infra_types.items():
        logging.info(f"Process table {table}")
        with open(path) as f:
            entities: list = json.load(f)
        # STEP 1: invalidate ALL present data and prevent user from interfering
        archive_everything(table)

        for entity in sorted(entities, key=lambda kv: kv['key']):
            central_value = entity['value']

            name = central_value['defaultName'] if has_default_name(table) else central_value['name']
            id_ = central_value['externalId'] if central_value.get('externalId') else central_value['externalID']
            uuid_ = central_value['uuid']
            logging.info(f"Processing {name}, {id_}, {uuid_}")
            # STEP 2 check if central item is locally present by uuid
            if update_by_local_uuid(table, central_value):
                continue
            if update_by_local_name_and_id(table, central_value):
                continue


def update_by_local_uuid(table, central_value):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            local = cur.execute(f"SELECT * FROM {table} WHERE uuid=%s", [central_value['uuid']]).fetchone()
            if local:
                perform_update(central_value, local, table, cur)
                logging.info(f"\t\tExact local uuid match de-archived and name/external id aligned!")
                return True
            else:
                logging.info(f"\t\tNo exact local uuid match! Continue with name and external id lookup!")
                return False


def update_by_local_name_and_id(table, central_value):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            where_ext_id, where_name = get_where_clause(central_value, table)

            if has_default_name(table):
                local = cur.execute(f"SELECT * FROM {table} WHERE defaultname=%s OR externalid=%s;",
                                    (where_name, where_ext_id)).fetchone()
            else:
                local = cur.execute(f"SELECT * FROM {table} WHERE name=%s OR externalid=%s;",
                                    (where_name, where_ext_id)).fetchone()

            if local:
                perform_update(central_value, local, table, cur)
            else:
                report_error(
                    f"\t\tCould not find local item present in central({central_value['uuid']}, {where_name}, {where_ext_id})")
                return False


def perform_update(central_value, local, table, cur):
    if not sanity_check(central_value, local, table):
        return

    local_uuid = local['uuid']
    local_name = get_local_name(local)
    local_ext_id = local['externalid']
    central_uuid = central_value['uuid']
    where_ext_id, where_name = get_where_clause(central_value, table)

    try:
        if has_default_name(table):
            cur.execute(
                f"UPDATE {table} SET uuid=%s,archived=FALSE,defaultname=%s,externalid=%s WHERE uuid=%s OR (defaultname=%s OR externalid=%s);",
                (central_uuid, where_name, where_ext_id, central_uuid, where_name, where_ext_id))
        else:
            cur.execute(
                f"UPDATE {table} SET uuid=%s,archived=FALSE,name=%s,externalid=%s WHERE uuid=%s OR (name=%s OR externalid=%s);",
                (central_uuid, where_name, where_ext_id, central_uuid, where_name, where_ext_id))

        uuid_changed = f"UUID: {local_uuid} -> {central_uuid}" if local_uuid != central_uuid else ""
        name_changed = f"Name: {local_name} -> {where_name}" if local_name != where_name else ""
        ext_id_changed = f"Ext. ID: {local_ext_id} -> {where_ext_id}" if local_ext_id != where_ext_id else ""

        logging.info(
            f"\t\tUpdated local item ({','.join([uuid_changed, name_changed, ext_id_changed])})")
        return True
    except UniqueViolation as e:
        report_error(
            f"\t\tCould not update UUID of {local_uuid}: {where_name}, {where_ext_id}"
            f"Either the name or external ID are present multiple times in the local DB.")
        return False


def get_local_name(local):
    return local['name'] if local.get('name') else local['defaultname']
    pass


def get_where_clause(central_value, table):
    where_name = central_value['defaultName'] if has_default_name(table) else central_value['name']
    where_ext_id = central_value['externalId'] if central_value.get('externalId') else central_value['externalID']
    return where_ext_id, where_name


def sanity_check(central_value, local, table):
    central_ext_id = central_value['externalId'] if central_value.get('externalId') else central_value[
        'externalID']
    local_ext_id = local['externalid']
    ext_id = central_ext_id == local_ext_id
    central_name = central_value[
        'defaultName' if has_default_name(table) else 'name']
    local_name = local[
        'defaultname' if has_default_name(table) else 'name']
    name = central_name == local_name

    # we allow either one to match
    # IMPORTANT: In the update we update both the name and external ID from central
    sanity = ext_id or name

    if not sanity:
        logging.info(
            f"\t\tSanity check failed for {table}: Central: {central_name}, {central_ext_id} and Local: {local_name}, {local_ext_id}")

    return sanity


def has_default_name(table):
    return table in ['continent', 'subcontinent', 'country']


def report_error(param):
    logging.error(param)
    error_list.append(f'{param.strip()}\n')


def main():
    iterate_central()
    logging.info("All done")
    with open('errors.log', 'w+') as f:
        f.writelines(error_list)


if __name__ == '__main__':
    main()
