from functools import partial

import sqlaload as sl

from common import *

def condense(engine, raw_table_name, resource_id, table_id):
    connection = engine.connect()
    trans = connection.begin()

    try:
        raw_table = sl.get_table(connection, raw_table_name)
        spending_table = sl.get_table(connection, 'spending')
        columns_table = sl.get_table(connection, 'columns')
        mappings = dict([(c.get('original'), c.get('column')) for c in \
                         sl.all(connection, columns_table) if c.get('valid')])
        for row in sl.all(connection, raw_table):
            spending_row = {'resource_id': resource_id,
                            'table_id': table_id,
                            'row_id': row['id'],}
            for key, value in row.items():
                if key not in mappings:
                    continue
                if not value or not len(value.strip()):
                    continue
                if mappings[key] in spending_row:
                    continue
                spending_row[mappings[key]] = value
            #print spending_row
            sl.upsert(connection, spending_table, spending_row, 
                      ['resource_id', 'table_id', 'row_id'])
        trans.commit()
    finally:
        connection.close()

def describe(raw_table_name):
    return 'condense: %s' % raw_table_name

def test_condense_all():
    engine = db_connect()
    table = sl.get_table(engine, 'extracted')
    for row in sl.all(engine, table):
        for table_id in xrange(0, row['max_table_id'] + 1):
            raw_table_name = 'raw_%s_table%s' % (row['resource_id'], table_id)
            if engine.has_table(raw_table_name):
                condense_ = partial(condense, engine, raw_table_name, row['resource_id'], table_id)
                condense_.description = describe(raw_table_name)
                yield condense_
