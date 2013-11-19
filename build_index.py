import sys
import sqlaload as sl

from common import *

log = logging.getLogger('build_index')

TAGS = ['spend-transactions', '25000', '25k', 'Spending Data', 'transactions']
TAGS = ['spend-transactions', '25k', '25000']
GROUPS = {}

def fetch_publisher(client, package):
    if not package['organization']:
        log.error("No organization: %r", package['name'])
        return {}
    return package['organization']

def fetch_package(client, package_name, engine, table, stats_resources):
    '''Queries CKAN for a particular dataset and stores metadata for each
    of its resources in the local database.'''
    try:
        pkg = client.action('package_show', id=package_name)
    except Exception, e:
        log.exception(e)
        return
    log.info("Dataset: %s", pkg['name'])
    publisher = fetch_publisher(client, pkg)
    # DGU splits resources into: timeseries, individual and additional
    # and we want to ignore additional (PDFs etc).
    resources = pkg.get('timeseries_resources', []) + \
                pkg.get('individual_resources', [])
    existing_rows = sl.find(engine, table, package_id=pkg['id'])
    processed_resource_ids = set()
    for res in resources:
        log.info(" > Resource %s: %s", res['id'], res['url'])
        data = {
            'resource_id': res['id'],
            'package_id': pkg['id'],
            'package_name': pkg['name'],
            'package_title': pkg['title'],
            'last_modified': res.get('last_modified'),
            'url': res['url'],
            'publisher_name': publisher.get('name'),
            'publisher_title': publisher.get('title'),
            'publisher_type': publisher.get('type'),
            'format': res['format'],
            'description': res['description']
            }
        row = sl.find_one(engine, table, resource_id=res['id'])
        processed_resource_ids.add(row['resource_id'])
        if row and row['url'] != data['url']:
            # url has changed, so force retrieval next time
            data['retrieve_status'] = False
            stats_resources.add_source('URL changed', data)
        elif row:
            stats_resources.add_source('URL unchanged', data)
        else:
            stats_resources.add_source('New resource', data)
        sl.upsert(engine, table, data, ['resource_id'])

    # Remove references to any deleted resources for this dataset
    obsolete_rows = [row for row in existing_rows
                     if row['resource_id'] not in processed_resource_ids]
    for row in obsolete_rows:
        sl.delete(engine, table, resource_id=row['resource_id'])
        sl.delete(engine, 'issue', resource_id=row['resource_id'])
        stats_resources.add_source('Deleted obsolete row', row)
    return len(resources)

def connect():
    engine = db_connect()
    src_table = sl.get_table(engine, 'source')
    return engine, src_table

def build_index(department_filter=None):
    '''Searches CKAN for spending resources and writes their metadata to
    the database.'''
    engine, table = connect()
    client = ckan_client()
    log.info('CKAN: %s', client.base_location)
    tags = ['+tags:"%s"' % t for t in TAGS]
    q = " OR ".join(tags)
    if department_filter:
        department_filter = ' OR '.join(['publisher:"%s"' % pub for pub in department_filter.split(',')])
        q = '(%s) AND (%s)' % (q, department_filter)
    log.info('Search q: %r', q)

    existing_packages = set(
            [res['package_name']
             for res in sl.distinct(engine, table, 'package_name')])
    processed_packages = set()
    res = client.package_search(q,
            search_options={'limit': 5})
    log.info('Search returned %i dataset results', res['count'])
    stats = OpenSpendingStats()
    stats_resources = OpenSpendingStats()
    for package_name in res['results']:
        processed_packages.add(package_name)
        num_resources = fetch_package(client, package_name, engine, table, stats_resources)
        if num_resources == 0:
            stats.add('No resources', package_name)
        else:
            stats.add('Found resources', package_name)
    # Removed rows about deleted packages
    for package_name in existing_packages - processed_packages:
        sl.delete(engine, table, package_name=package_name)
        sl.delete(engine, 'issue', package_name=package_name)
        stats.add('Removed obsolete dataset', package_name)
    print 'Datasets build_index summary:'
    print stats.report()
    print 'Resources build_index summary:'
    print stats_resources.report()

if __name__ == '__main__':
    if len(sys.argv) > 2:
        print 'Usage: python %s [<department-name>]' % sys.argv[0]
        sys.exit(1)
    elif len(sys.argv) == 2:
        department_filter = sys.argv[1]
    else:
        department_filter = None
    build_index(department_filter)
