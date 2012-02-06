import sys

import sqlaload as sl
import Levenshtein

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

def candidates(engine, columns_table, text):
    try:
        columns = [c.get('column') for c in sl.distinct(engine, columns_table, 'column')]
        columns = set(columns + PREDEFINED)
        columns = [(c, distance(text, c)) for c in columns]
        return sorted(columns, key=lambda (a,b): b)
    except KeyError:
        return []

def map_column(engine, columns_table, row):
    print "\nMatching: ", row['original']
    print "Context: ", row['columns']
    print "Common Values: ", row['examples']
    choices = candidates(engine, columns_table, row['original'])
    for i, (column, distance) in enumerate(choices):
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

def map_columns():
    #engine = sl.connect("sqlite:///uk25k.db")
    engine = sl.connect("postgresql://localhost/ukspending")
    columns_table = sl.get_table(engine, 'columns')
    for row in sl.all(engine, columns_table):
        if row.get('valid'):
            continue
        try:
            column = map_column(engine, columns_table, row)
            sl.upsert(engine, columns_table, 
                      {'original': row['original'],
                       #'resource_id': row['resource_id'],
                       'valid': True,
                       'column': column}, 
                      ['original'])
        except ValueError:
            sl.upsert(engine, columns_table, 
                      {'original': row['original'],
                       #'resource_id': row['resource_id'],
                       'valid': False}, 
                      ['original'])

if __name__ == '__main__':
    map_columns()
