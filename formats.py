from collections import defaultdict
from functools import partial
from datetime import datetime

import sqlaload as sl

NUMERIC_FIELDS = ['Amount', 'VATNumber']
DATE_FIELDS = ['Date']

FORMATS = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%m/%d/%Y']

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
        for format_ in FORMATS:
            try:
                datetime.strptime(value, format_)
                scores[format_] += weight
            except: pass
    scores = sorted(scores.items(), key=lambda (f,n): n)
    #print scores
    assert len(scores), ("No format found:", values)
    return scores[-1][0]


def format(engine, table, section):

    date_field_values = defaultdict(list)
    for row in sl.find(engine, table,
        resource_id=section.get('resource_id'),
        table_id=section.get('table_id')):
        for date_field in DATE_FIELDS:
            if date_field in row and row[date_field]:
                date_field_values[date_field].append(row[date_field])

    date_field_formats = {}
    for date_field, values in date_field_values.items():
        date_field_formats[date_field] = detect_date_format(values)

    for row in sl.find(engine, table,
        resource_id=section.get('resource_id'),
        table_id=section.get('table_id')):

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
                parsed = datetime.strptime(row[date_field], format_)
                row[date_field + 'AsDate'] = parsed.strftime("%Y-%m-%d")
            except Exception as e:
                print e

        sl.upsert(engine, table, row, 
                  ['resource_id', 'table_id', 'row_id'])

def test_format_all():
    engine = sl.connect("sqlite:///uk25k.db")
    table = sl.get_table(engine, 'spending')
    for row in sl.distinct(engine, table, 'resource_id', 'table_id'):
        format_ = partial(format, engine, table, row)
        format_.description = \
            'format: %(resource_id)s/%(table_id)s' % row
        yield format_







