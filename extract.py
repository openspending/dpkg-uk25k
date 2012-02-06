import os
import csv
import logging 
import traceback
from collections import defaultdict
from functools import partial

from messytables import *
import sqlaload as sl

from common import source_path

log = logging.getLogger(__name__)

def convert_(value):
    if not isinstance(value, basestring):
        value = unicode(value)
    elif isinstance(value, str):
        try:
            value = value.decode('utf-8')
        except:
            value = value.decode('latin-1')
    return value

def extract_table(engine, table, row, resource_id):
    assert os.path.exists(source_path(row)), "No source file exists."
    fh = open(source_path(row), 'rb')
    columns_table = sl.get_table(engine, 'columns')
    raw_table = sl.get_table(engine, 'raw')
    try:
        try:
            table_set = XLSTableSet.from_fileobj(fh)
        except Exception:
            fh.seek(0)
            table_set = CSVTableSet.from_fileobj(fh)

        for table_id, row_set in enumerate(table_set.tables):
            #types = type_guess(row_set.sample)
            #row_set.register_processor(types_processor(types))
            offset, headers = headers_guess(row_set.sample)
            headers = map(convert_, headers)
            assert len(headers)>1, "Only one column was detected; assuming this is not CSV."
            #print headers
            
            row_set.register_processor(headers_processor(headers))
            row_set.register_processor(offset_processor(offset + 1))

            values = defaultdict(lambda: defaultdict(int))

            for row_id, row_ in enumerate(row_set):
                cells = dict([(c.column, convert_(c.value)) for c in row_ if \
                    len(c.column.strip())])
                for cell, value in cells.items():
                    values[cell][value] += 1
                cells['_resource_id'] = row['resource_id']
                cells['_table_id'] = table_id
                cells['_row_id'] = row_id
                sl.upsert(engine, raw_table, cells, 
                    ['_resource_id', '_table_id', '_row_id'])
            
            for column in headers:
                if not len(column.strip()):
                    continue
                examples = sorted(values.get(column, {}).items(), 
                        key=lambda (a,b): b, reverse=True)[:7]
                examples = [a for (a, b) in examples]
                sl.upsert(engine, columns_table, {
                        'original': column,
                        #'resource_id': row['resource_id'],
                        'examples': unicode(examples),
                        'columns': unicode(headers)},
                    ['original']) #, 'resource_id'])
    #except Exception:
    #    traceback.print_exc()
    #    #log.exception(ex)
    #    assert False, traceback.format_exc()
    finally:
        fh.close()

def test_extract_all():
    engine = sl.connect("postgresql://localhost/ukspending")
    table = sl.get_table(engine, 'source')
    for row in sl.find(engine, table):
        extract = partial(extract_table, engine, table, row)
        extract.description = \
            'extract_table: %(package_name)s/%(resource_id)s %(url)s' % row
        yield extract, row['resource_id']

#if __name__ == '__main__':
#    #build_index(engine)
#    extract_all(engine)


