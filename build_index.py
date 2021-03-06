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
        processed_resource_ids.add(res['id'])
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

def build_index(publisher_name=None):
    '''Searches CKAN for spending resources and writes their metadata to
    the database.'''
    engine, table = connect()
    client = ckan_client()
    log.info('CKAN: %s', client.base_location)
    tags = ['+tags:"%s"' % t for t in TAGS]
    q = " OR ".join(tags)
    publisher_dict_filter = {}
    if publisher_name:
        publisher_solr_filter = 'publisher:"%s"' % publisher_name
        q = '(%s) AND (%s)' % (q, publisher_solr_filter)
        publisher_dict_filter = {'publisher_name': publisher_name}
    log.info('SOLR Search q: %r', q)

    existing_packages = set(
            [res['package_name']
             for res in sl.distinct(engine, table, 'package_name', **publisher_dict_filter)])
    log.info('Existing datasets: %i', len(existing_packages))
    processed_packages = set()
    log.info('Doing package search for: "%s"', q)
    res = client.package_search(q,
            search_options={'limit': 2000})
    log.info('Search returned %i dataset results', res['count'])
    stats = OpenSpendingStats()
    stats_resources = OpenSpendingStats()
    for package_name in res['results']:
        processed_packages.add(package_name)
        num_resources = fetch_package(client, package_name, engine, table, stats_resources)
        if num_resources == 0:
            stats.add('Dataset has no resources', package_name)
        else:
            stats.add('Dataset has resources', package_name)
    # Removed rows about deleted packages
    obsolete_packages = existing_packages - processed_packages
    log.info('Obsolete datasets: %s from %s',
             len(obsolete_packages), len(existing_packages))
    for package_name in obsolete_packages:
        sl.delete(engine, table, package_name=package_name)
        sl.delete(engine, 'issue', package_name=package_name)
        stats.add('Removed obsolete dataset', package_name)
    # Removed stray rows without package_name
    stray_rows = list(sl.find(engine, table, package_name=None))
    if stray_rows:
        log.info('Stray rows without package_name: %i',
                 len(stray_rows))
        sl.delete(engine, table, package_name=None)
        sl.delete(engine, 'issue', package_name=None)
        for row in stray_rows:
            stats.add('Stray row removed', row['resource_id'])
    print 'Datasets build_index summary:'
    print stats.report()
    print 'Resources build_index summary:'
    print stats_resources.report()

if __name__ == '__main__':
    if len(sys.argv) > 2:
        print 'Usage: python %s [<publisher-name>]' % sys.argv[0]
        sys.exit(1)
    elif len(sys.argv) == 2:
        publisher_name = sys.argv[1]
    else:
        publisher_name = None
    build_index(publisher_name)
