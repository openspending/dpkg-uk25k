import sqlaload as sl
import logging
import sys
import time

from common import *
from sqlalchemy import select, func

publishers = {}

def scan(engine, resource_id, table_id, publisher):
    table_suffix = '%s_table%s' % (resource_id, table_id)

    if not engine.has_table('raw_%s' % table_suffix):
        return

    raw_table = sl.get_table(engine, 'raw_%s' % table_suffix)
    normalised_headers = ','.join(normalise_columns_list(raw_table))

    if sl.find_one(engine, raw_table) is None:
        # Skip tables that contain no rows
        return

    column_list_table = sl.get_table(engine, 'column_list')
    sl.upsert(engine, column_list_table, {'resource_id': resource_id, 'table_id': table_id, 'normalised': normalised_headers}, ['resource_id', 'table_id'])
    
    #publishers.setdefault(publisher, []).append(sorted())

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    engine = db_connect()

    table = sl.get_table(engine, 'extracted')
    source_table = sl.get_table(engine, 'source')
    for row in sl.all(engine, table):
        source_row = sl.find_one(engine, source_table, resource_id=row['resource_id'])
        for table_id in xrange(0, row['max_table_id'] + 1):
            scan(engine, row['resource_id'], table_id, source_row['publisher'])

    column_sets_table = sl.get_table(engine, 'column_sets')
    column_list_table = sl.get_table(engine, 'column_list')

    # Populate column_sets from column_list, with counts
    q = select([column_list_table.c.normalised, func.count(column_list_table.c.id)], group_by='normalised')
    result = engine.execute(q)
    for normalised, count in result:
        sl.upsert(engine, column_sets_table, {'normalised': normalised, 'count': int(count)}, ['normalised'])

    #for publisher in publishers:
    #    print publisher
    #    for cols in publishers[publisher]:
    #        print "  %s" % u' , '.join(cols).encode('utf-8')
