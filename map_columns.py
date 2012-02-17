import sys

import sqlaload as sl
import Levenshtein
import re
import json
import traceback
from tempfile import NamedTemporaryFile

from common import *
from sqlalchemy import select

PREDEFINED = [u'SupplierName', u'DepartmentFamily', u'Entity', u'Amount',
    u'ExpenseType', u'ExpenseArea', u'VATNumber', u'Date', u'TransactionNumber']

aliases = {'VATNumber': ['vatregistrationnumber', 'suppliervatregistrationnumber', 'vatregno', 'vatregistration', 'vatno'],
           'TransactionNumber': ['transno', 'transactionno'],
           'Date': ['dateofpayment', 'transactiondate', 'paymentdate'],
           'Amount': ['amountinsterling'],
           'SupplierName': ['merchantname', 'supplier'],
           'DepartmentFamily': ['department', 'deptfamily']
           }

# Add all the identities to aliases, so we can use it for lookup
for k in PREDEFINED:
    aliases.setdefault(k, []).append(normalise_header(k))

# Find the distance to the closest alias
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
            # First, assign all literal matches
            if to_norm == from_col:
                mapping[from_col] = to_col
                left_over.remove(to_col)
                unmapped.remove(from_col)
            else:
                distances.setdefault(from_col, {})[to_col] = alias_distance(from_col, to_col)

    # Then, assign each predefined name to the closest match
    while len(left_over) and len(unmapped):
        distance_list = []
        for to_col in left_over:
            unmapped_tmp = unmapped.copy()
            while len(unmapped_tmp) > 0:
                from_col = unmapped_tmp.pop()
                distance_list.append( (distances[from_col][to_col], from_col, to_col) )

        d, f, t = min(distance_list, key=lambda x: x[0])
        if d > 5:
            break

        mapping[f] = t
        left_over.remove(t)
        unmapped.remove(f)
    
    return mapping, list(left_over)

def prompt_for(cols, mapping, left_over, count):
    print
    print "Matching: %s" % ', '.join(cols)
    if count is not None:
        print "Used in %d tables" % count
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
        print >>temp, "# -*- coding: utf-8 -*-"
        print >>temp, "# Lines beginning with # are ignored, as are blank lines"
        print >>temp, "# Other lines should be of the form 'normalisedinputname: StandardName'"
        print >>temp, ''

        for col in cols:
            print >>temp, (u'%s: %s' % (col, mapping.get(col, ''))).encode('utf-8')

        print >>temp, ''
        print >>temp, '# Unassigned standard names:'
        for col in sorted(left_over):
            print >>temp, '# %s' % col.encode('utf-8')

        temp.flush()
        x = os.spawnlp(os.P_WAIT,editor,editor,temp.name)
        if x != 0:
            print "Error %d from editor!" % x

        temp.seek(0)
        mapping = {}
        left_over = set(PREDEFINED)
        for line in temp:
            line = unicode(line, 'utf-8').strip()
            if line.startswith('#') or len(line) == 0:
                continue
            from_name, to_name = line.split(':')
            from_name = from_name.strip()
            to_name = to_name.strip()
            if len(to_name):
                mapping[from_name] = to_name
                left_over.discard(to_name)
        return mapping, list(left_over)

def map_column(columns_table, normalised, count):
    cols = filter(lambda c: len(c) > 0, normalised.split(','))

    mapping,left_over = assign_defaults(cols)

    while True:
        line = prompt_for(cols, mapping, left_over, count)
        if line == 'a':
            return mapping
        elif line == 'e':
            mapping,left_over = edit_mapping(cols, mapping, left_over)
        elif line == 's':
            raise ValueError('Skipping')
        elif line == 'b':
            return None
        elif line == 'q':
            sys.exit(0)

def connect():
    engine = db_connect()
    columns_table = sl.get_table(engine, 'column_sets')
    return engine,columns_table

def map_columns():
    engine, columns_table = connect()

    q = columns_table.select(order_by=[columns_table.c.count.desc().nullslast()])
    rows = []

    # Finish the query before we start updating
    for row in engine.execute(q):
        if row.has_key('valid'):
            if row['valid'] is not None:
                continue
        rows.append(row)

    for row in rows:
        normalised = row['normalised']
        count = row['count']
        try:
            columns = map_column(columns_table, normalised, count)
            if columns is not None:
                sl.upsert(engine, columns_table, 
                          {'normalised': normalised,
                           'valid': True,
                           'column_map': json.dumps(columns)},
                          ['normalised'])
            else:
                sl.upsert(engine, columns_table, 
                          {'normalised': normalised,
                           'valid': False}, 
                          ['normalised'])
        except SystemExit:
            raise
        except:
            traceback.print_exc()

if __name__ == '__main__':
    map_columns()
