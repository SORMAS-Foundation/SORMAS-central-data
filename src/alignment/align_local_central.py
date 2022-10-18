import os

import psycopg
import json
import argparse

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row

import logging

error_list = []

FORMAT = '%(message)s'

logFormatter = logging.Formatter(FORMAT)
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("output.log")
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)
rootLogger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument("-H", "--host", default=os.environ.get("host"), help="database server host or socket directory",
                    action="store")
parser.add_argument("-P", "--port", default=os.environ.get("PORT"), help="port to connect to", action="store")
parser.add_argument("-d", "--dbname", default=os.environ.get("dbname"), help="database name to connect to)",
                    action="store")
parser.add_argument("-u", "--username", default=os.environ.get("username"),
                    help="database user name",
                    action="store")
parser.add_argument("-p", "--password", default=os.environ.get("password"), help="password for user", action="store")
parser.add_argument("-i", "--input", default=os.environ.get("input"), help="path where to expect the central data",
                    action="store")
parser.add_argument("-B", "--bavarian", default=os.environ.get("bavarian"), help="activate the bavarian mode",
                    action="store")
parser.add_argument("-a", "--archive", default=os.environ.get("ARCHIVE"), help="archive on conflict",
                    action="store")
parser.add_argument("-t", "--test", default=os.environ.get("test"), help="test mode/dry run", action="store")

args, unknown = parser.parse_known_args()

assert len(unknown) == 0

CONNECTION = f"host={args.host} dbname={args.dbname} user={args.username} password={args.password} port={args.port}"
logging.info(f'Connecting to {args.host}')
PATH = args.input
BAVARIAN_MODE = args.bavarian == 'true'
ARCHIVE_ON_CONFLICT = args.archive == 'true'
DRY_RUN = args.test == "true"

NUMBER_OF_NAMES = {}


def archive_everything(table):
    if DRY_RUN:
        return
    logging.info(f"Archive {table}")
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("UPDATE featureconfiguration SET enabled=FALSE WHERE featuretype='EDIT_INFRASTRUCTURE_DATA';")
            cur.execute(f"UPDATE {table} SET archived=TRUE WHERE TRUE;")


def compute_community_names(table, items):
    if table != "community":
        return

    # compute the numbers of names for each community
    for item in items:
        name = item['value']['name']
        if name in NUMBER_OF_NAMES:
            NUMBER_OF_NAMES[name] += 1
        else:
            NUMBER_OF_NAMES[name] = 1


def warn_about_missing_communities(table, central_value):
    if table != "community":
        return
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # count the number of communities with central_value name
            name_ = central_value['name']
            cur.execute("SELECT count(*) FROM community WHERE name=%s;", (name_,))
            count = cur.fetchone()['count']
            central_number = NUMBER_OF_NAMES[name_]
            if count != central_number:
                report_error(f"Number of community name {name_} differs: Central {central_number}, Local {count}")


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

        l = list(sorted(entities, key=lambda kv: kv['key']))
        length = len(l)

        compute_community_names(table, l)

        for index, entity in enumerate(l):
            central_value = entity['value']
            name = central_value['defaultName'] if has_default_name(table) else central_value['name']
            id_ = central_value['externalId'] if central_value.get('externalId') else central_value['externalID']
            uuid_ = central_value['uuid']
            logging.info(f"{index + 1}/{length}: Processing {name}, {id_}, {uuid_}")

            warn_about_missing_communities(table, central_value)

            # STEP 2: check if central item is locally present by uuid
            if update_by_local_uuid(table, central_value):
                continue
            if update_by_local_name_and_id(table, central_value):
                continue
            if table == "country":
                update_by_local_iso_and_uno_code(table, central_value)


def update_by_local_iso_and_uno_code(table, central_value):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            where_ext_id, where_name = get_where_clause(central_value, table)
            iso_code, uno_code = central_value['isoCode'], central_value['unoCode']
            local = cur.execute(
                f"SELECT * FROM {table} WHERE defaultname=%s OR externalid=%s OR isocode=%s OR unocode=%s;",
                (where_name, where_ext_id, iso_code, uno_code)).fetchone()
            if local is None:
                logging.info(f"Could not find {central_value} locally")
                return

            local_uuid = local['uuid']
            local_name = get_local_name(local)
            local_ext_id = local['externalid']
            central_uuid = central_value['uuid']
            iso_code, uno_code = central_value['isoCode'], central_value['unoCode']
            try:
                if DRY_RUN:
                    if cur.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE uuid=%s OR (defaultname=%s OR externalid=%s OR isocode=%s OR unocode=%s);",
                            (central_uuid, where_name, where_ext_id, iso_code, uno_code)).fetchone()['count'] > 1:
                        raise UniqueViolation()
                else:
                    cur.execute(
                        f"UPDATE {table} SET uuid=%s,archived=FALSE,defaultname=%s,externalid=%s, isocode=%s, unocode=%s WHERE uuid=%s OR (defaultname=%s OR externalid=%s OR isocode=%s OR unocode=%s);",
                        (central_uuid, where_name, where_ext_id, iso_code, uno_code, central_uuid, where_name,
                         where_ext_id,
                         iso_code, uno_code))
                if not DRY_RUN:
                    uuid_changed = f"UUID: {local_uuid} -> {central_uuid}" if local_uuid != central_uuid else ""
                    name_changed = f"Name: {local_name} -> {where_name}" if local_name != where_name else ""
                    ext_id_changed = f"Ext. ID: {local_ext_id} -> {where_ext_id}" if local_ext_id != where_ext_id else ""
                    iso_code_changed = f"ISO: {local['isocode']} -> {iso_code}" if local['isocode'] != iso_code else ""
                    uno_code_changed = f"UNO: {local['unocode']} -> {uno_code}" if local['unocode'] != uno_code else ""
                    logging.info(
                        f"\t\tUpdated local item ({','.join([uuid_changed, name_changed, ext_id_changed, iso_code_changed, uno_code_changed])})")
                    return True
            except UniqueViolation:
                report_error(
                    f"\t\tCould not update UUID of {local_uuid}: {where_name}, {where_ext_id}, {local['isocode']},{local['unocode']} "
                    f"Either the name or external ID are present multiple times in the local DB.")
                return False


def update_by_local_uuid(table, central_value):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            local = cur.execute(f"SELECT * FROM {table} WHERE uuid=%s", [central_value['uuid']]).fetchone()
            if local:
                perform_update_uuid(central_value, local, table, conn)
                if not DRY_RUN:
                    logging.info(f"\t\tExact local uuid match de-archived and name/external id aligned!")
                return True
            else:
                if not DRY_RUN:
                    logging.info(f"\t\tNo exact local uuid match! Continue with name and external id lookup!")
                return False


def update_by_local_name_and_id(table, central_value):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            where_ext_id, where_name = get_where_clause(central_value, table)

            if has_default_name(table):
                local = cur.execute(f"SELECT * FROM {table} WHERE defaultname=%s OR externalid=%s;",
                                    (where_name, where_ext_id)).fetchall()
            else:
                local = cur.execute(f"SELECT * FROM {table} WHERE name=%s OR externalid=%s;",
                                    (where_name, where_ext_id)).fetchall()

            if len(local) == 1:
                return perform_update_name_or_id(central_value, local[0], table, conn)
            elif len(local) > 1:
                return fix_duplicates(central_value, table, conn)
            else:
                report_error(
                    f"\t\tCould not find local item present in central({central_value['uuid']}, {where_name}, {where_ext_id})")
                return False


def perform_update_uuid(central_value, local, table, conn):
    with conn.cursor(row_factory=dict_row) as cur:
        if not sanity_check(central_value, local, table):
            return

        local_uuid = local['uuid']
        local_name = get_local_name(local)
        local_ext_id = local['externalid']
        central_uuid = central_value['uuid']
        where_ext_id, where_name = get_where_clause(central_value, table)

        try:
            if has_default_name(table):
                if DRY_RUN:
                    if cur.execute(f"SELECT COUNT(*) FROM {table}  WHERE uuid=%s;", (central_uuid,)).fetchone()[
                        'count'] > 1:
                        report_error(
                            f"Duplicate in {table} WHERE uuid={central_uuid}")
                else:
                    cur.execute(
                        f"UPDATE {table} SET uuid=%s,archived=FALSE,defaultname=%s,externalid=%s WHERE uuid=%s;",
                        (central_uuid, where_name, where_ext_id, central_uuid))
            else:

                if DRY_RUN:
                    if cur.execute(f"SELECT COUNT(*) FROM {table}  WHERE uuid=%s;", (central_uuid,)).fetchone()[
                        'count'] > 1:
                        report_error(
                            f"Duplicate in {table} WHERE uuid={central_uuid}")
                else:
                    cur.execute(
                        f"UPDATE {table} SET uuid=%s,archived=FALSE,name=%s,externalid=%s WHERE uuid=%s;",
                        (central_uuid, where_name, where_ext_id, central_uuid))
            if not DRY_RUN:
                uuid_changed = f"UUID: {local_uuid} -> {central_uuid}" if local_uuid != central_uuid else ""
                name_changed = f"Name: {local_name} -> {where_name}" if local_name != where_name else ""
                ext_id_changed = f"Ext. ID: {local_ext_id} -> {where_ext_id}" if local_ext_id != where_ext_id else ""

                logging.info(
                    f"\t\tUpdated local item ({','.join([uuid_changed, name_changed, ext_id_changed])})")
            return True
        except UniqueViolation as e:
            # we have duplicates for name OR external id
            return fix_duplicates(central_value, table, conn)


def perform_update_name_or_id(central_value, local, table, conn):
    with conn.cursor(row_factory=dict_row) as cur:
        if not sanity_check(central_value, local, table):
            return

        local_uuid = local['uuid']
        local_name = get_local_name(local)
        local_ext_id = local['externalid']
        central_uuid = central_value['uuid']
        where_ext_id, where_name = get_where_clause(central_value, table)

        try:
            if has_default_name(table):
                if DRY_RUN:
                    if cur.execute(
                            f"SELECT COUNT(*) FROM {table}  WHERE defaultname=%s OR externalid=%s;",
                            (central_uuid, where_name, where_ext_id, where_name, where_ext_id)).fetchone()['count'] > 1:
                        report_error(
                            f"\t\tDuplicate in {table} WHERE defaultname={where_name} OR externalid={where_ext_id}")
                        raise UniqueViolation
                else:
                    cur.execute(
                        f"UPDATE {table} SET uuid=%s,archived=FALSE,defaultname=%s,externalid=%s WHERE defaultname=%s OR externalid=%s;",
                        (central_uuid, where_name, where_ext_id, where_name, where_ext_id))
            else:
                if DRY_RUN:
                    if cur.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE name=%s OR externalid=%s;",
                            (where_name, where_ext_id)).fetchone()['count'] > 1:
                        report_error(
                            f"\t\tDuplicate in {table} WHERE name={where_name} OR externalid={where_ext_id}")
                        raise UniqueViolation
                else:
                    cur.execute(
                        f"UPDATE {table} SET uuid=%s,archived=FALSE,name=%s,externalid=%s WHERE name=%s OR externalid=%s;",
                        (central_uuid, where_name, where_ext_id, where_name, where_ext_id))
            if not DRY_RUN:
                uuid_changed = f"UUID: {local_uuid} -> {central_uuid}" if local_uuid != central_uuid else ""
                name_changed = f"Name: {local_name} -> {where_name}" if local_name != where_name else ""
                ext_id_changed = f"Ext. ID: {local_ext_id} -> {where_ext_id}" if local_ext_id != where_ext_id else ""

                logging.info(
                    f"\t\tUpdated local item ({','.join([uuid_changed, name_changed, ext_id_changed])})")
                return True
        except UniqueViolation:
            # we have duplicates for name OR external id
            return fix_duplicates(central_value, table, conn)


def fix_duplicates(central_value, table, conn):
    with conn.cursor(row_factory=dict_row) as cur:
        where_ext_id, where_name = get_where_clause(central_value, table)
        if has_default_name(table):
            count = cur.execute(f"SELECT COUNT(*) FROM {table} WHERE defaultname=%s OR externalid=%s;",
                                (where_name, where_ext_id)).fetchone()['count']

            true_duplicates = cur.execute(f"SELECT * FROM {table} WHERE defaultname=%s AND externalid=%s;",
                                          (where_name, where_ext_id)).fetchall()

        else:
            count = cur.execute(f"SELECT COUNT(*) FROM {table} WHERE name=%s OR externalid=%s;",
                                (where_name, where_ext_id)).fetchone()['count']
            true_duplicates = cur.execute(f"SELECT * FROM {table} WHERE name=%s AND externalid=%s;",
                                          (where_name, where_ext_id)).fetchall()

        logging.info(f"\tfound {count} duplicates of {where_name} or {where_ext_id} in {table}")
        logging.info(f"\tfound true {true_duplicates} duplicates of {where_name} and {where_ext_id} in {table}")

        if len(true_duplicates) == 0:
            return try_resolve_duplicates(central_value, table, conn)

        elif len(true_duplicates) == 1:
            # we have one match for name AND external id
            assert len(true_duplicates) == 1
            central_uuid = central_value['uuid']
            if has_default_name(table):
                if DRY_RUN:
                    if cur.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE defaultname=%s AND externalid=%s;",
                            (where_name, where_ext_id)).fetchone()['count'] > 1:
                        report_error(
                            f"\t\tDuplicate in {table} WHERE defaultname={where_name} AND externalid={where_ext_id}")
                else:
                    cur.execute(
                        f"UPDATE {table} SET uuid=%s,archived=FALSE,defaultname=%s,externalid=%s WHERE defaultname=%s AND externalid=%s;",
                        (central_uuid, where_name, where_ext_id, where_name, where_ext_id))

            else:
                if cur.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE name=%s AND externalid=%s;",
                        (where_name, where_ext_id)).fetchone()['count'] > 1:
                    report_error(
                        f"\t\tDuplicate in {table} WHERE name={where_name} AND externalid={where_ext_id}")
                else:
                    cur.execute(
                        f"UPDATE {table} SET uuid=%s,archived=FALSE,name=%s,externalid=%s WHERE name=%s AND externalid=%s;",
                        (central_uuid, where_name, where_ext_id, where_name, where_ext_id))
            if not DRY_RUN:
                local_uuid = true_duplicates[0]['uuid']
                local_ext_id = true_duplicates[0]['externalid']
                local_name = true_duplicates[0]['defaultname'] if has_default_name(table) else true_duplicates[0][
                    'name']

                uuid_changed = f"UUID: {local_uuid} -> {central_uuid}" if local_uuid != central_uuid else ""
                name_changed = f"Name: {local_name} -> {where_name}" if local_name != where_name else ""
                ext_id_changed = f"Ext. ID: {local_ext_id} -> {where_ext_id}" if local_ext_id != where_ext_id else ""

                logging.info(
                    f"\t\tUpdated local item ({','.join([uuid_changed, name_changed, ext_id_changed])})")
            return True
        else:
            return try_resolve_duplicates(central_value, table, conn)


def bavarian_mode(central_value, table, conn):
    with conn.cursor(row_factory=dict_row) as cur:
        assert table == "community"
        can_join = {}
        central_uuid = central_value['uuid']
        central_name = central_value['name']
        central_ext_id = central_value['externalID']

        with conn.cursor(row_factory=dict_row) as cur:
            res = cur.execute(f"SELECT id FROM {table} WHERE name=%s OR externalid=%s;",
                              (central_name, central_ext_id)).fetchall()

            for duplicate in res:
                # join the community table with the facility table on the id of the duplicate
                join = cur.execute(
                    f"SELECT community.id from community JOIN facility f on community.id = f.community_id "
                    f"WHERE f.community_id = %s;", (duplicate['id'],)).fetchone()

                can_join[duplicate['id']] = True if join else False

            # count the number of True values in the can_join dict
            num_can_join = sum(can_join.values())

            if num_can_join == 0:
                report_manual_cleanup(central_value, table)

            if num_can_join > 1:
                report_manual_cleanup(central_value, table)

            match = None
            # get the element from the can_join dict where the value is True
            for key, value in can_join.items():
                if value:
                    match = key
                    break
            assert match

            local = cur.execute("SELECT * FROM community WHERE id=%s;", (match,)).fetchone()

            if DRY_RUN:
                report_error(f"\t\tUpdating {match} in bavarian mode")
                return True

            cur.execute(
                f"UPDATE {table} SET uuid=%s,archived=FALSE,name=%s,externalid=%s WHERE id=%s;",
                (central_uuid, central_name, central_ext_id, match))

            local_uuid = local['uuid']
            local_ext_id = local['externalid']
            local_name = local['name']

            uuid_changed = f"UUID: {local_uuid} -> {central_uuid}" if local_uuid != central_uuid else ""
            name_changed = f"Name: {local_name} -> {central_name}" if local_name != central_name else ""
            ext_id_changed = f"Ext. ID: {local_ext_id} -> {central_ext_id}" if local_ext_id != central_ext_id else ""

            logging.info(
                f"\t\tUpdated local item ({','.join([uuid_changed, name_changed, ext_id_changed])})")

            return True


def report_manual_cleanup(central_value, table):
    msg = f"Multiple duplicates found for {central_value} in {table}. THIS CANNOT BE FIXED AUTOMATICALLY. PLEASE " \
          f"CLEANUP MANUALLY AND THEN RESTART THE SCRIPT. " \
          f"Run SELECT * FROM {table} WHERE name='{central_value['name']}' " \
          f"OR externalid='{central_value['externalID']}' to see the duplicates."

    if DRY_RUN:
        report_error(msg)
        return False

    if ARCHIVE_ON_CONFLICT:
        msg += "Archiving the duplicate."
        logging.warning(msg)
        return False
    else:
        raise Exception(msg)


def try_resolve_duplicates(central_value, table, conn):
    if BAVARIAN_MODE:
        bavarian_mode(central_value, table, conn)
    else:
        report_manual_cleanup(central_value, table)


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
        report_error(
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
