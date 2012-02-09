
import sqlaload as sl

from ckanclient import CkanClient
from common import *


def fetch_package(client, package_name, engine, table):
    print package_name
    pkg = client.package_entity_get(package_name)
    for res in pkg['resources']:
        sl.upsert(engine, table, {
            'resource_id': res['id'],
            'package_id': pkg['id'],
            'package_name': pkg['name'],
            'url': res['url'],
            'publisher': pkg.get('extras', {}).get('published_by'),
            'format': res['format'],
            'description': res['description']
            }, ['resource_id'])

def connect():
    engine = db_connect()
    src_table = sl.get_table(engine, 'source')
    return engine,src_table

def test_build_index():
    engine, table = connect()
    client = CkanClient(base_location='http://catalogue.data.gov.uk/api')
    res = client.package_search("tags:spend-transactions",
            search_options={'limit': 5})
    for package_name in res['results']:
        fetch_package.description = 'metadata: %s' % package_name
        yield fetch_package, client, package_name, engine, table

#if __name__ == '__main__':
#    build_index(engine)
