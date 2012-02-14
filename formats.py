from collections import defaultdict
from functools import partial
from datetime import datetime
from xlrd.xldate import xldate_as_tuple

import sqlaload as sl
import logging
import sys
import time

from common import *

NUMERIC_FIELDS = ['Amount', 'VATNumber']
DATE_FIELDS = ['Date']

FORMATS = [
    # Variations on sensible date formats
    '%Y-%m-%d', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
    # Less sensible date formats
    '%d/%m/%Y', '%d/%m/%Y %H:%M', '%m/%d/%Y', '%d/%m/%y', '%d.%m.%y', '%d.%m.%Y',
    # Ridiculous ones
    '%m/%d/%y', '%Y%m%d',
    # Things with words in
    '%d-%b-%y', '%d-%b-%Y', '%d/%b/%Y', '%b-%y', '%d %B %Y', '%d %b %y',
    # Things we'd prefer to forget
    'excel']

def detect_date_format(values):
    # TODO: alternative solution - some sheets use more than one date format, 
    # could pass a priorized list and attempt each?
    if not len(values):
        return None
    values_ = defaultdict(int)
    for value in values:
        values_[value] += 1

    scores = defaultdict(int)
    for (value, weight) in values_.items():
        if value is None:
            continue
        for format_ in FORMATS:
            try:
                if format_ == 'excel':
                    # Since it's the only integer format supported, this isn't bad. It's 1982 .. 2036 ish
                    assert float(value) > 30000 and float(value) < 50000
                else:
                    datetime.strptime(value.strip(), format_)
                scores[format_] += weight
            except: pass
    scores = sorted(scores.items(), key=lambda (f,n): n)
    #print scores
    assert len(scores), ("No format found:", values)
    return scores[-1][0]


def do_format(engine, resource_id, table_id):
    table_suffix = '%s_table%s' % (resource_id, table_id)

    table = sl.get_table(engine, 'spending_%s' % table_suffix)

    date_field_values = defaultdict(list)
    for row in sl.all(engine, table):
        for date_field in DATE_FIELDS:
            if date_field in row and row[date_field]:
                date_field_values[date_field].append(row[date_field])

    date_field_formats = {}
    for date_field, values in date_field_values.items():
        date_field_formats[date_field] = detect_date_format(values)

    for row in sl.all(engine, table):

        for numeric_field in NUMERIC_FIELDS:
            try:
                val = row.get(numeric_field)
                if val is None:
                    continue
                val = "".join([v for v in val if v in "-.0123456789"])
                row[numeric_field + 'AsFloat'] = float(val)
            except Exception as e:
                print e

        for date_field, format_ in date_field_formats.items():
            if format_ is None:
                continue
            try:
                if row[date_field] is None:
                    continue
                if format_ == 'excel':
                    # Deciphers excel dates that have been mangled into integers by formatting errors
                    parsed = datetime(*xldate_as_tuple(float(row[date_field].strip()), 0))
                else:
                    parsed = datetime.strptime(row[date_field].strip(), format_)
                row[date_field + 'AsDate'] = parsed.strftime("%Y-%m-%d")
            except Exception as e:
                print e

        sl.upsert(engine, table, row, ['id'])

def format(engine, resource_id, table_id, force):
    connection = engine.connect()
    trans = connection.begin()
    condensed_table = sl.get_table(connection, 'condensed')
    condensed_row = sl.find_one(connection, condensed_table, resource_id=resource_id, table_id=table_id)
    if condensed_row is None:
        condensed_row = {'resource_id': resource_id,
                         'table_id': table_id,
                         }
    start = time.time()
    try:
        if not force and condensed_row['format_time'] is not None:
            return
        do_format(connection, resource_id, table_id)
        condensed_row['format_time'] = time.time() - start
        sl.upsert(connection, condensed_table, condensed_row, ['resource_id', 'table_id'])
        trans.commit()
    finally:
        connection.close()

def connect():
    engine = db_connect()
    table = sl.get_table(engine, 'spending')
    return engine,table

def test_format_all():
    engine,table = connect()
    table = sl.get_table(engine, 'condensed')
    for row in sl.all(engine, table):
        format_ = partial(format, engine, row['resource_id'], row['table_id'], False)
        format_.description = \
            'format: %(resource_id)s/%(table_id)s' % row
        yield format_

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    engine,table = connect()
    format(engine, sys.argv[1], sys.argv[2], True)
