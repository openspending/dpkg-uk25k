import os
import csv
import logging 
import traceback
import sys
import time
import chardet
import codecs
import re
from collections import defaultdict
from functools import partial

from messytables import *
import sqlaload as sl

from common import *

log = logging.getLogger(__name__)

def keyify(key):
    # None of these characters can be used in column names, due to sqlalchemy bugs
    key = key.replace('\r', '')
    key = key.replace('\n', ' ')
    key = key.replace('(', '[')
    key = key.replace(')', ']')
    return key

def convert_(value):
    if not isinstance(value, basestring):
        value = unicode(value)
    elif isinstance(value, str):
        try:
            value = value.decode('utf-8')
        except:
            value = value.decode('latin-1')
    return value

html_re = re.compile(r'<!doctype|<html', re.I)
COMPDOC_SIGNATURE = "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"

def extract_table(engine, table, row, resource_id, force):
    # For now, interpret lack of data as not-failure at this stage, on
    # the basis that it was already reported as failure at the
    # retrieve stage and will just clutter up this list.
    if not os.path.exists(source_path(row)):
        return
    #assert os.path.exists(source_path(row)), "No source file exists."
    
    connection = engine.connect()
    extracted_table = sl.get_table(connection, 'extracted')

    # Skip over tables we have already extracted
    if not force and sl.find_one(engine, extracted_table, resource_id=resource_id) is not None:
        return

    fh = open(source_path(row), 'rb')
    source_data = fh.read()

    assert len(source_data) > 0, "Empty file"
    assert html_re.search(source_data[0:1024]) is None, "Looks like HTML"
    assert not source_data.startswith('%PDF'), "Looks like PDF"

    trans = connection.begin()
    start = time.time()
    try:
        if source_data.startswith(COMPDOC_SIGNATURE):
            fh.seek(0)
            table_set = XLSTableSet.from_fileobj(fh)
        elif source_data.startswith('PK'):
            table_set = XLSXTableSet(source_path(row))
        else:
            cd = chardet.detect(source_data)
            fh.close()
            fh = codecs.open(source_path(row), 'r', cd['encoding'])

            table_set = CSVTableSet.from_fileobj(fh)

        for table_id, row_set in enumerate(table_set.tables):
            #types = type_guess(row_set.sample)
            #row_set.register_processor(types_processor(types))
            offset, headers = headers_guess(row_set.sample)
            headers = map(convert_, headers)
            assert len(headers)>1 or len(table_set.tables) > 1, "Only one column was detected; assuming this is not valid data."
            #print headers

            # We might have multiple table sets where one is blank or ranty text or something. Skip those.
            if len(headers) <= 1:
                continue
            
            row_set.register_processor(headers_processor(headers))
            row_set.register_processor(offset_processor(offset + 1))

            values = defaultdict(lambda: defaultdict(int))

            raw_table_name = 'raw_%s_table%s' % (resource_id, table_id)
            sl.drop_table(connection, raw_table_name)
            raw_table = sl.get_table(connection, raw_table_name)

            for row_ in row_set:
                cells = dict([(keyify(c.column), convert_(c.value)) for c in row_ if \
                    len(c.column.strip())])
                for cell, value in cells.items():
                    values[cell][value] += 1
                sl.add_row(connection, raw_table, cells)

        sl.upsert(connection, extracted_table, {'resource_id': resource_id,
                                                'max_table_id': table_id,
                                                'extraction_time': time.time() - start,
                                                }, ['resource_id'])

        trans.commit()
    #except Exception:
    #    traceback.print_exc()
    #    #log.exception(ex)
    #    assert False, traceback.format_exc()
    finally:
        connection.close()
        fh.close()

def connect():
    engine = db_connect()
    src_table = sl.get_table(engine, 'source')
    return engine,src_table

def describe(row):
    return 'extract_table: %(package_name)s/%(resource_id)s %(url)s' % row

def test_extract_all():
    engine, table = connect()
    for row in sl.find(engine, table):
        extract = partial(extract_table, engine, table, row)
        extract.description = describe(row)
        yield extract, row['resource_id'], False

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    engine,table = connect()
    for id in sys.argv[1:]:
        row = sl.find_one(engine, table, resource_id=id)
        if row is None:
            print "Could not find row %s" % id
        else:
            print describe(row)
            extract_table(engine, table, row, row['resource_id'], True)
