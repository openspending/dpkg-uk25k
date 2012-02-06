from functools import partial

import sqlaload as sl

def condense(engine, raw_table, raw_row):
    spending_table = sl.get_table(engine, 'spending')
    columns_table = sl.get_table(engine, 'columns')
    mappings = dict([(c.get('original'), c.get('column')) for c in \
                     sl.all(engine, columns_table) if c.get('valid')])
    for row in sl.find(engine, raw_table,
        _resource_id=raw_row.get('_resource_id'),
        _table_id=raw_row.get('_table_id')):
        
        spending_row = {'resource_id': row['_resource_id'],
                        'table_id': row['_table_id'],
                        'row_id': row['_row_id'],}
        for key, value in row.items():
            if key not in mappings:
                continue
            if not value or not len(value.strip()):
                continue
            if mappings[key] in spending_row:
                continue
            spending_row[mappings[key]] = value
        #print spending_row
        sl.upsert(engine, spending_table, spending_row, 
                  ['resource_id', 'table_id', 'row_id'])

def test_condense_all():
    engine = sl.connect("sqlite:///uk25k.db")
    table = sl.get_table(engine, 'raw')
    for row in sl.distinct(engine, table, '_resource_id', '_table_id'):
        condense_ = partial(condense, engine, table, row)
        condense_.description = \
            'condense: %(_resource_id)s/%(_table_id)s' % row
        yield condense_




