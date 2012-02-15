from itertools import chain

import sqlaload as sl
import logging
import sys
import time
import json
import urllib
import urllib2
import multiprocessing
from contextlib import closing

from common import *

def connect():
    engine = db_connect()
    table = sl.get_table(engine, 'condensed')
    return engine,table

rows_count = 0
tables_count = 0
suppliers_visited = {}
def supplier_names_from_table(engine, resource_id, table_id):
    global tables_count, rows_count
    tables_count = tables_count + 1
    print "# checking table %d" % tables_count
    table_suffix = '%s_table%s' % (resource_id, table_id)

    table = sl.get_table(engine, 'spending_%s' % table_suffix)
    supplier_table = sl.get_table(engine, 'suppliers')

    for row in sl.all(engine, table):
        rows_count = rows_count + 1
        if not row.has_key('SupplierName'):
            # One of the junk tables that contain no real data, usually excel noise
            continue
        supplier = row['SupplierName']

        if supplier is None or supplier == '':
            continue

        if suppliers_visited.has_key(supplier):
            continue

        suppliers_visited[supplier] = True

        if sl.find_one(engine, supplier_table, original=supplier) is not None:
            continue

        yield supplier

def supplier_tables(engine, table):
    for row in sl.all(engine, table):
        yield supplier_names_from_table(engine, row['resource_id'], row['table_id'])

def supplier_names(engine, table):
    return chain.from_iterable(supplier_tables(engine, table))

def lookup_supplier_name(name):
    try:
        query = {'query': name, 'limit': 1}
        url = "http://opencorporates.com/reconcile?%s" % urllib.urlencode({'query': json.dumps(query)})
        with closing(urllib2.urlopen(url), None, 30) as f:
            data = json.loads(f.read())

        if len(data['result']) > 0:
            return {'original': name,
                    'result': data['result'][0],
                    }
    except:
        return None

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)

    engine, table = connect()
    supplier_table = sl.get_table(engine, 'suppliers')

    pool = multiprocessing.Pool(40)

    fails = 0

    for r in pool.imap_unordered(lookup_supplier_name, supplier_names(engine, table)):
        if r is not None:
            print "%s ==> %s" % (r['original'], r['result']['name'])
            sl.upsert(engine, supplier_table, {'original': r['original'],
                                               'name': r['result']['name'],
                                               'uri': r['result']['uri'],
                                               'score': r['result']['score'],
                                               },
                      ['original'])
            print "# %d rows and %d tables visited" % (rows_count, tables_count)
        else:
            fails = fails + 1
            if fails % 100 == 0:
                print "# %d requests failed" % fails
