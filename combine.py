import time

import sqlaload as sl
import nkclient as nk

from common import *

log = logging.getLogger('combine')
KEY_CACHE = {}

def normalize(column_name):
    column_name = column_name.replace('.', '')
    column_name = column_name.replace(',', '')
    column_name = column_name.replace('  ', ' ')
    column_name = column_name.replace('no.', 'Number')
    column_name = column_name.replace('No.', 'Number')
    return column_name.strip()

def column_mapping(row, columns):
    nkc = nk_connect('uk25k-column-names')
    columns.remove('id')
    if not len(columns):
        return None
    sheet_signature = '|'.join(sorted(set(map(normalize, columns))))
    mapping = {}
    failed = False
    for column in columns:
        key = '%s @ [%s]' % (normalize(column), sheet_signature)
        if key in KEY_CACHE:
            mapping[column] = KEY_CACHE[key]
            continue
        try:
            v = nkc.lookup(key)
            mapping[column] = v.value
        except nk.NKInvalid, nm:
            mapping[column] = None
        except nk.NKNoMatch, nm:
            failed = True
        KEY_CACHE[key] = mapping.get(column)
    return None if failed else mapping

def combine_sheet(engine, resource, sheet_id, table, mapping):
    begin = time.time()
    base = {
            'resource_id': resource['resource_id'],
            'resource_hash': resource['extract_hash'],
            'sheet_id': sheet_id,
        }
    spending_table = sl.get_table(engine, 'spending')
    rows = 0
    for row in sl.all(engine, table):
        data = dict(base)
        for col, value in row.items():
            if col == 'id':
                data['row_id'] = value
                continue
            mapped = mapping.get(col)
            if mapped is not None:
                data[mapped] = value
        sl.upsert(engine, spending_table, data, 
                unique=['resource_hash', 'sheet_id', 'row_id'])
        rows += 1
    log.info("Loaded %s rows in %s ms", rows,
            int((time.time()-begin)*1000))
    return rows > 0

def combine_resource_core(engine, row):
    success = True
    for sheet_id in range(0, row['sheets']):
        table = sl.get_table(engine, 'raw_%s_sheet%s' % (
            row['resource_id'], sheet_id))
        if not engine.has_table(table):
            log.warn("Sheet table does not exist: %s", table)
            success = False
            continue
        columns = [c.name for c in table.columns]
        mapping = column_mapping(row, columns)
        if mapping is None:
            log.warn("Unable to generate mapping: %r", columns)
            success = False
            continue
        if not combine_sheet(engine, row, sheet_id, table, mapping):
            succces = False
    return success, ""

def combine_resource(engine, source_table, row, force):
    if not row['extract_status']:
        return

    # Skip over tables we have already combineed
    if not force and sl.find_one(engine, source_table,
            resource_id=row['resource_id'],
            combine_hash=row['extract_hash']) is not None:
        return

    log.info("Combine: %s, Resource %s", row['package_name'], row['resource_id'])

    status, message = combine_resource_core(engine, row)
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'combine_hash': row['extract_hash'],
        'combine_status': status,
        'combine_message': message,
        }, unique=['resource_id'])

def combine_all(force=False):
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table):
        combine_resource(engine, source_table, row, force)

if __name__ == '__main__':
    combine_all(False)

