import os

from collections import defaultdict
from datetime import datetime

import psycopg
import json
import argparse

import logging

from psycopg.rows import dict_row

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

args, unknown = parser.parse_known_args()

assert len(unknown) == 0

CONNECTION = f"host={args.host} dbname={args.dbname} user={args.username} password={args.password} port={args.port}"
logging.info(f'Connecting to {args.host}')
PATH = args.input

NUMBER_OF_NAMES: dict[str, int] = {}


def insert_missing(group):
    with psycopg.connect(CONNECTION) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            present_communities = cur.execute("SELECT * FROM community WHERE name = %s", (group[0],)).fetchall()

            for present_community in present_communities:
                district_id = present_community['district_id']
                # fetch district
                district = cur.execute("SELECT uuid,name FROM district WHERE id = %s", (district_id,)).fetchone()
                assert district is not None
                # find the community in the group that has the same district
                community = next(filter(lambda x: x['district']['uuid'] == district['uuid'], group[1]), None)
                assert community is not None
                # update the uuid of the present community
                cur.execute("UPDATE community SET uuid = %s WHERE id = %s",
                            (community['uuid'], present_community['id']))
                logging.info(f"Updated community {present_community} in district {district['name']} to uuid {community['uuid']}")

            # insert the remaining communities
            for community in group[1]:
                count = cur.execute("SELECT COUNT(*) FROM community WHERE uuid = %s", (community['uuid'],)).fetchone()[
                    'count']
                if count == 0:
                    max_id = cur.execute(f"SELECT max(id) FROM community").fetchone()['max']

                    name: str = community['name']
                    external_id: str = community['externalID']
                    # force download again
                    date = datetime.fromisoformat('2000-01-01').strftime("%Y-%m-%d %H:%M:%S")
                    district_id = \
                    cur.execute("SELECT id FROM district WHERE uuid = %s", (community['district']['uuid'],)).fetchone()[
                        'id']
                    assert district_id is not None

                    cur.execute(f"""
                    insert into community 
                    (id, changedate, creationdate, name, uuid, district_id, archived, externalid,  centrally_managed, sys_period) 
                    values ('{max_id + 1}','{date}', '{date}', %s, '{community['uuid']}', {district_id}, false, {external_id},true, '["{date}",)');
                    """, (name,))
                    logging.info(f"Inserted central value: {community}")


def main():
    community_path = f'{PATH}/germany/community.json'
    with open(community_path) as f:
        community_values = map(lambda x: x['value'], json.load(f))
        # group community values by name
        grouped_by_name = defaultdict(list)
        for community in community_values:
            grouped_by_name[community['name']].append(community)

        groups = list(filter(lambda x: len(x[1]) > 1, grouped_by_name.items()))
        for group in groups:
            insert_missing(group)


if __name__ == '__main__':
    main()
