
import sqlaload as sl

from ckanclient import CkanClient
from common import *

log = logging.getLogger('build_index')

TAGS = ['spend-transactions', '25000', '25k', 'Spending Data', 'transactions']
GROUPS = {}

def fetch_group(client, package):
    if len(package['groups']) != 1:
        log.warn("Invalid groups: %r", package['groups'])
        return {}
    group_name = package['groups'].pop()
    if group_name not in GROUPS:
        GROUPS[group_name] = client.group_entity_get(group_name)
    return GROUPS[group_name]

def fetch_package(client, package_name, engine, table):
    pkg = client.package_entity_get(package_name)
    log.info("Dataset: %s", pkg['name'])
    group = fetch_group(client, pkg)
    for res in pkg['resources']:
        log.info(" > Resource %s: %s", res['id'], res['url'])
        sl.upsert(engine, table, {
            'resource_id': res['id'],
            'package_id': pkg['id'],
            'package_name': pkg['name'],
            'package_title': pkg['title'],
            'last_modified': res.get('last_modified'),
            'url': res['url'],
            'publisher_name': group.get('name'),
            'publisher_title': group.get('title'),
            'publisher_type': group.get('type'),
            'format': res['format'],
            'description': res['description']
            }, ['resource_id'])

def connect():
    engine = db_connect()
    src_table = sl.get_table(engine, 'source')
    return engine, src_table

def build_index():
    engine, table = connect()
    tags = ['+tags:"%s"' % t for t in TAGS]
    client = CkanClient(base_location='http://data.gov.uk/api')
    res = client.package_search(" OR ".join(tags),
            search_options={'limit': 5})
    for package_name in res['results']:
        fetch_package(client, package_name, engine, table)

if __name__ == '__main__':
    build_index()
