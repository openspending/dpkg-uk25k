import sys
import sqlaload as sl

from common import *

log = logging.getLogger('build_index')

TAGS = ['spend-transactions', '25000', '25k', 'Spending Data', 'transactions']
TAGS = ['spend-transactions', '25k', '25000']
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
    '''Queries CKAN for a particular dataset and stores metadata for each
    of its resources in the local database.'''
    try:
        pkg = client.package_entity_get(package_name)
    except Exception, e:
        log.exception(e)
        return
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

def build_index(department_filter=None):
    '''Searches CKAN for spending resources and writes their metadata to
    the database.'''
    engine, table = connect()
    client = ckan_client()
    tags = ['+tags:"%s"' % t for t in TAGS]
    q = " OR ".join(tags)
    if department_filter:
        department_filter = ' OR '.join(['publisher:"%s"' % pub for pub in department_filter.split(',')])
        q = '(%s) AND (%s)' % (q, department_filter)
    log.info('Search q: %r', q)

    res = client.package_search(q,
            search_options={'limit': 5})
    log.info('Search returned %i dataset results', res['count'])
    for package_name in res['results']:
        fetch_package(client, package_name, engine, table)

if __name__ == '__main__':
    if len(sys.argv) > 2:
        print 'Usage: python %s [<department-name>]' % sys.argv[0]
        sys.exit(1)
    elif len(sys.argv) == 2:
        department_filter = sys.argv[1]
    else:
        department_filter = None
    build_index(department_filter)
