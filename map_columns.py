import sys

import sqlaload as sl
import Levenshtein
import re
import json
from tempfile import NamedTemporaryFile

from common import *

PREDEFINED = [u'SupplierName', u'DepartmentFamily', u'Entity', u'Amount',
    u'ExpenseType', u'ExpenseArea', u'VATNumber', u'Date', u'TransactionNumber']

aliases = {'VATNumber': ['vatregistrationnumber'],
           'TransactionNumber': ['transno'],
           }

for k in PREDEFINED:
    aliases.setdefault(k, []).append(normalise_header(k))

def candidates(engine, columns_table, text):
    try:
        columns = [c.get('column') for c in sl.distinct(engine, columns_table, 'column') if c.get('column') is not None]
    except KeyError:
        columns = []
        
    columns = set(columns + PREDEFINED)
    columns = [(c, distance(text, c), normalised(c)) for c in columns]
    return sorted(columns, key=lambda (a,b,c): b)

def alias_distance(from_col, to_col):
    distances = map(lambda x: Levenshtein.distance(from_col, unicode(x)), aliases[to_col])
    return min(distances)

def assign_defaults(cols):
    mapping = {}
    left_over = set(PREDEFINED)
    unmapped = set(cols)

    distances = {}

    for to_col in list(left_over):
        to_norm = normalise_header(to_col)
        for from_col in list(unmapped): 
            if to_norm == from_col:
                mapping[from_col] = to_col
                left_over.remove(to_col)
                unmapped.remove(from_col)
            else:
                distances.setdefault(from_col, {})[to_col] = alias_distance(from_col, to_col)

    while len(left_over):
        distance_list = []
        for to_col in left_over:
            for from_col in unmapped:
                distance_list.append( (distances[from_col][to_col], from_col, to_col) )

        d, f, t = min(distance_list, key=lambda x: x[0])
        if d > 5:
            break

        mapping[f] = t
        left_over.remove(t)
        unmapped.remove(f)
    
    return mapping, list(left_over)

def prompt_for(cols, mapping, left_over):
    print
    print "Matching: %s" % ', '.join(cols)
    print "Mapping:"
    for col in cols:
        print "  %s: %s" % (col, mapping.get(col, ''))
    print "No mapping for: %s" % ', '.join(sorted(left_over))

    sys.stdout.write("\n[A]ccept, [E]dit, mark as [B]roken, [S]kip, or [Q]uit? ")
    line = ''
    while line not in ['a', 'e', 'b', 's', 'q']:
        line = sys.stdin.readline().strip().lower()
    return line

def edit_mapping(cols, mapping, left_over):
    editor = os.getenv('EDITOR', None)
    if editor is None:
        print "Please set $EDITOR first"
        return mapping, left_over

    with NamedTemporaryFile() as temp:
        print >>temp, "# Lines beginning with # are ignored, as are blank lines"
        print >>temp, "# Other lines should be of the form 'normalisedinputname: StandardName'"
        print >>temp, ''

        for col in cols:
            print >>temp, '%s: %s' % (col, mapping.get(col, ''))

        print >>temp, ''
        print >>temp, '# Unassigned standard names:'
        for col in sorted(left_over):
            print >>temp, '# %s' % col

        temp.flush()
        x = os.spawnlp(os.P_WAIT,editor,editor,temp.name)
        if x != 0:
            print "Error %d from editor!" % x

        temp.seek(0)
        mapping = {}
        left_over = set(PREDEFINED)
        for line in temp:
            line = line.strip()
            if line.startswith('#') or len(line) == 0:
                continue
            from_name, to_name = line.split(':')
            from_name = from_name.strip()
            to_name = to_name.strip()
            mapping[from_name] = to_name
            left_over.discard(to_name)
        return mapping, list(left_over)

def map_column(engine, columns_table, row):
    normalised = row['normalised']
    cols = normalised.split(',')

    mapping,left_over = assign_defaults(cols)

    while True:
        line = prompt_for(cols, mapping, left_over)
        if line == 'a':
            return mapping
        elif line == 'e':
            mapping,left_over = edit_mapping(cols, mapping, left_over)
        elif line == 's':
            return None
        elif line == 'b':
            raise ValueError()
        elif line == 'q':
            sys.exit(0)

def connect():
    engine = db_connect()
    columns_table = sl.get_table(engine, 'column_sets')
    return engine,columns_table

def map_columns():
    engine, columns_table = connect()
    for row in sl.all(engine, columns_table):
        if row.get('valid'):
            continue
        try:
            columns = map_column(engine, columns_table, row)
            if columns is not None:
                sl.upsert(engine, columns_table, 
                          {'normalised': row['normalised'],
                           'valid': True,
                           'column_map': json.dumps(columns)},
                          ['normalised'])
        except ValueError:
            sl.upsert(engine, columns_table, 
                      {'normalised': row['normalised'],
                       'valid': False}, 
                      ['normalised'])

if __name__ == '__main__':
    map_columns()
