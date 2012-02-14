import sqlaload as sl
import csv
import sys
import logging
from common import *

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)

    supplier_map = {}

    engine = db_connect()
    supplier_table = sl.get_table(engine, 'suppliers')
    for row in sl.all(engine, supplier_table):
        supplier_map[ row['original'] ] = row

    writer = csv.DictWriter(open(sys.argv[1], 'wb'),
                            ['SupplierName', 'DepartmentFamily', 'Entity',
                             'ExpenseType', 'ExpenseArea', 'VATNumber',
                             'AmountAsFloat',
                             'DateAsDate',
                             'SupplierGuess', 'SupplierGuessURI',
                             'Resource', 'Table',
                             'Publisher', 'SourceDescription',
                             ],
                            extrasaction='ignore')
    writer.writeheader()

    source_table = sl.get_table(engine, 'source')
    condensed_table = sl.get_table(engine, 'condensed')
    for condensed_row in sl.all(engine, condensed_table):
        resource_id = condensed_row['resource_id']
        table_id = condensed_row['table_id']
        table_suffix = '%s_table%s' % (resource_id, table_id)
        source_row = sl.find_one(engine, source_table, resource_id=resource_id)
        publisher = source_row['publisher']
        source_description = source_row['description']

        table = sl.get_table(engine, 'spending_%s' % table_suffix)
        for row in sl.all(engine, table):
            # Skip over rows that contain no usable data
            if not row.has_key('SupplierName') or row.get('AmountAsFloat') is None or row.get('DateAsDate') is None:
                continue

            # Bring in all the other bits of data we've been collecting
            row['Resource'] = resource_id
            row['Table'] = table_id
            supplier_guess = supplier_map.get(row['SupplierName'])
            if supplier_guess is not None:
                row['SupplierGuess'] = supplier_guess['name']
                row['SupplierGuessURI'] = supplier_guess['uri']
            row['Publisher'] = publisher
            row['SourceDescription'] = source_description

            # We need to flatten the unicode objects into utf8, because python csv is braindead
            writer.writerow({k:unicode(v).encode('utf8') for k,v in row.iteritems()})
