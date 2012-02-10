import sys

import sqlaload as sl
import Levenshtein
import re

from common import *

PREDEFINED = [u'SupplierName', u'DepartmentFamily', u'Entity', u'Amount',
    u'ExpenseType', u'ExpenseArea', u'VATNumber', u'Date']

def clean_str(text):
    text = text or u""
    return text.replace(' ', '').lower().strip()

def distance(text, candidate):
    return Levenshtein.distance(
            clean_str(text),
            clean_str(candidate)
            )

def normalised(c):
    return c.lower().replace(' ', '').replace('.', '')

column_map = {}

def candidates(engine, columns_table, text):
    try:
        columns = [c.get('column') for c in sl.distinct(engine, columns_table, 'column') if c.get('column') is not None]
    except KeyError:
        columns = []
        
    columns = set(columns + PREDEFINED)
    columns = [(c, distance(text, c), normalised(c)) for c in columns]
    return sorted(columns, key=lambda (a,b,c): b)

def map_column(engine, columns_table, row):
    original = row['original']

    # Filter out some rubbish
    if len(original) > 100:
        raise ValueError()

    if re.match(r'^[\d,.]*$', original):
        raise ValueError()
    
    if re.match(r'^\d+/\d+/\d+$', original):
        raise ValueError()

    choices = candidates(engine, columns_table, original)
    norm_original = normalised(original)

    # Reuse duplicates up to normalisation
    if column_map.has_key(norm_original):
        if column_map[norm_original]['valid']:
            return column_map[norm_original]['column']
        else:
            raise ValueError()

    # If we have an exact match up to normalisation, then go with it
    for column, _, n in choices:
        if n == norm_original:
            return column

    print "\nMatching: ", row['original']
    print "Context: ", row['columns']
    print "Common Values: ", row['examples']
    for i, (column, distance, _) in enumerate(choices):
        print " [%s]: %s (%s)" % (i, column, distance)
    sys.stdout.write("\nPress 'x' for invalid, number for choice, or enter new.\n> ")
    line = sys.stdin.readline().strip()
    if not len(line):
        line = '0'
    if line == 'x':
        raise ValueError()
    try:
        index = int(line)
        return choices[index][0]
    except:
        return line
    return None

def connect():
    engine = db_connect()
    columns_table = sl.get_table(engine, 'columns')
    return engine,columns_table

def map_columns():
    engine, columns_table = connect()
    for row in sl.all(engine, columns_table):
        if row.get('column') is not None or row.get('valid') is not None:
            column_map[normalised(row['original'])] = {'column': row.get('column'), 'valid': row.get('valid')}
    for row in sl.all(engine, columns_table):
        if row.get('valid'):
            continue
        try:
            column = map_column(engine, columns_table, row)
            if column is not None:
                column_map[normalised(row['original'])] = {'column': column, 'valid': True}
                sl.upsert(engine, columns_table, 
                          {'original': row['original'],
                           #'resource_id': row['resource_id'],
                           'valid': True,
                           'column': column}, 
                          ['original'])
        except ValueError:
            column_map[normalised(row['original'])] = {'column': None, 'valid': False}
            sl.upsert(engine, columns_table, 
                      {'original': row['original'],
                       #'resource_id': row['resource_id'],
                       'valid': False}, 
                      ['original'])

if __name__ == '__main__':
    map_columns()
