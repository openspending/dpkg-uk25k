import os
import csv
import logging 
import sys
import time
import chardet
import codecs
import re
from collections import defaultdict

from messytables import *
import sqlaload as sl

from common import *
from common import issue as _issue

log = logging.getLogger('extract')

def issue(engine, resource_id, resource_hash, message, data={}):
    _issue(engine, resource_id, resource_hash, 'extract',
           message, data=data)

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

def extract_resource_core(engine, row, stats):
    connection = engine.connect()
    fh = open(source_path(row), 'rb')
    source_data = fh.read()

    if not len(source_data):
        issue(engine, row['resource_id'], row['retrieve_hash'],
              "Empty file")
        stats.add_source('Empty file', row)
        return False, 0
    if html_re.search(source_data[0:1024]) is not None:
        issue(engine, row['resource_id'], row['retrieve_hash'],
              "HTML file detected, not a transaction report")
        stats.add_source('HTML file', row)
        return False, 0
    if source_data.startswith('%PDF'):
        issue(engine, row['resource_id'], row['retrieve_hash'],
              "PDF file detected, not a transaction report")
        stats.add_source('PDF file', row)
        return False, 0

    trans = connection.begin()
    start = time.time()
    try:
        if source_data.startswith(COMPDOC_SIGNATURE):
            fh.seek(0)
            table_set = XLSTableSet(fh)
        elif source_data.startswith('PK'):
            table_set = XLSXTableSet(source_path(row))
        else:
            #fh.seek(0)
            from StringIO import StringIO
            sio = StringIO(source_data)

            encoding = None
            detected = chardet.detect(source_data[:200])
            if detected.get('encoding') == 'ISO-8859-2' and '\xa3' in source_data:
                # Detected as Latin2 but probably isn't - that is for Eastern
                # European languages.  Probably because the presence of a GBP
                # pound sign has foxed chardet. It is pretty certain that it is
                # a single-byte ASCII-variant, and my money is on Windows-1252
                encoding = 'windows-1252'

            table_set = CSVTableSet(sio, encoding=encoding)

        sheets = 0
        for sheet_id, row_set in enumerate(table_set.tables):
            offset, headers = headers_guess(row_set.sample)
            headers = map(convert_, headers)
            log.debug("Headers: %r", headers)
            if len(headers) <= 1:
                continue
            sheets += 1

            row_set.register_processor(headers_processor(headers))
            row_set.register_processor(offset_processor(offset + 1))

            values = defaultdict(lambda: defaultdict(int))

            raw_table_name = 'raw_%s_sheet%s' % (row['resource_id'], sheet_id)
            sl.drop_table(connection, raw_table_name)
            raw_table = sl.get_table(connection, raw_table_name)

            for row_ in row_set:
                cells = dict([(keyify(c.column), convert_(c.value)) for c in row_ if \
                    len(c.column.strip())])
                for cell, value in cells.items():
                    values[cell][value] += 1
                sl.add_row(connection, raw_table, cells)

        trans.commit()
        log.debug(stats.add_source('Extracted ok', row))
        return sheets>0, sheets
    except Exception, ex:
        log.exception(ex)
        issue(engine, row['resource_id'], row['retrieve_hash'],
              unicode(ex))
        stats.add_source('Exception: %s' % ex.__class__.__name__, row)
        return False, 0
    finally:
        log.debug("Processed in %sms", int(1000*(time.time() - start)))
        connection.close()
        fh.close()

def extract_resource(engine, source_table, row, force, stats):
    if not row['retrieve_status']:
        stats.add_source('Previous step (retrieve) not complete', row)
        log.debug('Row has no retrieve status - skipping')
        return

    # Skip over tables we have already extracted
    if not force and sl.find_one(engine, source_table,
            resource_id=row['resource_id'],
            extract_status=True,
            extract_hash=row['retrieve_hash']) is not None:
        stats.add_source('Already extracted', row)
        return

    log.info("Extracting: %s, File %s", row['package_name'], row['resource_id'])

    status, sheets = extract_resource_core(engine, row, stats)
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'extract_hash': row['retrieve_hash'],
        'extract_status': status,
        'sheets': sheets
        }, unique=['resource_id'])

def extract_some(force=False, **kwargs):
    # kwargs = resource_id=x, package_name=y, publisher_title=z
    stats = OpenSpendingStats()
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table, **kwargs):
        extract_resource(engine, source_table, row, force, stats)
    log.info('Extract summary: \n%s' % stats.report())

def extract_all(force=False):
    extract_some(force=force)

if __name__ == '__main__':
    args = sys.argv[1:]
    filter = {}
    if '-h' in args or '--help' in args or len(args) > 2:
        print 'Usage: python %s [<resource ID>]' % sys.argv[0]
        sys.exit(1)
    elif len(args) == 1:
        filter = {'resource_id': args[0]}

    extract_some(force=False, **filter)

