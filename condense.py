from functools import partial

import sqlaload as sl
import logging
import sys
import time

from common import *

def condense(engine, resource_id, table_id, force):
    table_suffix = '%s_table%s' % (resource_id, table_id)

    if not engine.has_table('raw_%s' % table_suffix):
        return

    condensed_table = sl.get_table(engine, 'condensed')

    # Skip over tables we have already extracted
    if not force and sl.find_one(engine, condensed_table, resource_id=resource_id, table_id=table_id) is not None:
        return

    connection = engine.connect()
    trans = connection.begin()

    start = time.time()

    try:
        raw_table = sl.get_table(connection, 'raw_%s' % table_suffix)
        sl.drop_table(connection, 'spending_%s' % table_suffix)
        spending_table = sl.get_table(connection, 'spending_%s' % table_suffix)
        columns_table = sl.get_table(connection, 'columns')
        mappings = dict([(c.get('original'), c.get('column')) for c in \
                         sl.all(connection, columns_table) if c.get('valid')])
        for row in sl.all(connection, raw_table):
            spending_row = {}
            for key, value in row.items():
                if key not in mappings:
                    continue
                if not value or not len(value.strip()):
                    continue
                if mappings[key] in spending_row:
                    continue
                spending_row[mappings[key]] = value
            #print spending_row
            sl.add_row(connection, spending_table, spending_row)
        sl.upsert(connection, condensed_table, {'resource_id': resource_id,
                                                'table_id': table_id,
                                                'condense_time': time.time() - start,
                                                }, ['resource_id', 'table_id'])
        trans.commit()
    finally:
        connection.close()

def describe(resource_id, table_id):
    return 'condense: %s %s' % (resource_id, table_id)

def test_condense_all():
    engine = db_connect()
    table = sl.get_table(engine, 'extracted')
    for row in sl.all(engine, table):
        for table_id in xrange(0, row['max_table_id'] + 1):
            condense_ = partial(condense, engine, row['resource_id'], table_id, False)
            condense_.description = describe(row['resource_id'], table_id)
            yield condense_

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    engine = db_connect()
    condense(engine, sys.argv[1], sys.argv[2], True)
