import time
import sys

import sqlaload as sl
import nomenklatura

from common import *
from common import issue as _issue

log = logging.getLogger('combine')
KEY_CACHE = {}

def issue(engine, resource_id, resource_hash, message, data={}):
    _issue(engine, resource_id, resource_hash, 'combine',
           message, data=data)

def normalize(column_name):
    column_name = column_name.replace('.', '')
    column_name = column_name.replace(',', '')
    column_name = column_name.replace('  ', ' ')
    column_name = column_name.replace('no.', 'Number')
    column_name = column_name.replace('No.', 'Number')
    return column_name.strip()

def normalize_hard(column_name):
    column_name = normalize(column_name)
    column_name = column_name.replace('_', '')
    column_name = column_name.replace('-', '')
    column_name = column_name.lower().replace(' ', '')
    return {
        'amount': 'Amount',
        'amountinsterling': 'Amount',
        'gross': 'Amount',
        'departmentfamily': 'DepartmentFamilyName',
        'departmentalfamily': 'DepartmentFamilyName',
        'deptfamily': 'DepartmentFamilyName',
        'department': 'DepartmentFamilyName',
        'departmentname': 'DepartmentFamilyName',
        'date': 'Date',
        'dateofpayment': 'Date',
        'paymentdate': 'Date',
        'transactiondate': 'Date',
        'entity': 'EntityName',
        'contractnumber': 'Contract Number',
        'transactionnumber': 'TransactionNumber',
        'transactionnr': 'TransactionNumber',
        'transactionno': 'TransactionNumber',
        'transno': 'TransactionNumber',
        'expensearea': 'ExpenseArea',
        'expensesarea': 'ExpenseArea',
        'expensetype': 'ExpenseType',
        'expensestype': 'ExpenseType',
        'expendituretype': 'ExpenditureType',
        'description': 'Narrative',
        'narrative': 'Narrative',
        'supplier': 'SupplierName',
        'suppliername': 'SupplierName',
        'suppliertype': 'SupplierType',
        'vatregistrationnumber': 'SupplierVATNumber',
        'vatno': 'SupplierVATNumber',
        'supplierpostcode': 'SupplierPostalCode',
        'postcode': 'SupplierPostalCode',
        'projectcode': 'ProjectCode'
        }.get(column_name)

def column_mapping(engine, row, columns):
    nkc = nk_connect('uk25k-column-names')
    columns.remove('id')
    sheet_signature = '|'.join(sorted(set(map(normalize, columns))))
    mapping = {}
    failed_columns = []
    for column in columns:
        if column.startswith("column_"):
            mapping[column] = None
            continue
        try:
            key = normalize_hard(column)
            if key is not None:
                mapping[column] = key
                continue
            key = '%s @ [%s]' % (normalize(column), sheet_signature)
            if key in KEY_CACHE:
                mapping[column] = KEY_CACHE[key]
                continue
            try:
                v = nkc.lookup(key)
                mapping[column] = v.name
            except nkc.Invalid, nm:
                mapping[column] = None
            except nkc.NoMatch, nm:
                failed_columns.append(column)
            KEY_CACHE[key] = mapping.get(column)
        except Exception, e:
            log.exception(e)
            failed_columns.append(column)
            mapping[column] = None
    if failed_columns:
        issue(engine, row['resource_id'], row['retrieve_hash'],
              'Column(s) not recognised', failed_columns)
    if not len([(k,v) for k,v in mapping.items() if v is not None]):
        return None
    return None if failed_columns else mapping

def combine_sheet(engine, resource, sheet_id, table, mapping):
    begin = time.time()
    base = {
            'resource_id': resource['resource_id'],
            'resource_hash': resource['extract_hash'],
            'sheet_id': sheet_id,
        }
    spending_table = sl.get_table(engine, 'spending')
    connection = engine.connect()
    trans = connection.begin()
    try:
        rows = 0
        sl.delete(connection, spending_table,
                resource_id=resource['resource_id'],
                sheet_id=sheet_id)
        for row in sl.all(connection, table):
            data = dict(base)
            for col, value in row.items():
                if col == 'id':
                    data['row_id'] = value
                    continue
                mapped = mapping.get(col)
                if mapped is not None:
                    data[mapped] = value
            sl.add_row(connection, spending_table, data)
            rows += 1
        trans.commit()
        log.info("Loaded %s rows in %s ms", rows,
                int((time.time()-begin)*1000))
        return rows > 0
    finally:
        connection.close()

def combine_resource_core(engine, row):
    success = True
    for sheet_id in range(0, row['sheets']):
        table = sl.get_table(engine, 'raw_%s_sheet%s' % (
            row['resource_id'], sheet_id))
        if not engine.has_table(table.name):
            log.warn("Sheet table does not exist: %s", table)
            success = False
            continue
        columns = [c.name for c in table.columns]
        mapping = column_mapping(engine, row, columns)
        if mapping is None:
            log.warn("Unable to generate mapping: %r", columns)
            success = False
            continue
        if not combine_sheet(engine, row, sheet_id, table, mapping):
            succces = False
    return success

def combine_resource(engine, source_table, row, force):
    if not row['extract_status']:
        return

    # Skip over tables we have already combined
    if not force and sl.find_one(engine, source_table,
            resource_id=row['resource_id'],
            combine_hash=row['extract_hash'],
            combine_status=True) is not None:
        return

    log.info("Combine: %s, Resource %s", row['package_name'], row['resource_id'])

    status = combine_resource_core(engine, row)
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'combine_hash': row['extract_hash'],
        'combine_status': status,
        }, unique=['resource_id'])

def combine_resource_id(resource_id, force=False):
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table, resource_id=resource_id):
        combine_resource(engine, source_table, row, force)

def combine(force=False, filter=None):
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table, **(filter or {})):
        combine_resource(engine, source_table, row, force)

if __name__ == '__main__':
    args = sys.argv[1:]
    filter = {}
    if '-h' in args or '--help' in args or len(args) > 2:
        print 'Usage: python %s [<resource ID>]' % sys.argv[0]
        sys.exit(1)
    elif len(args) == 1:
        filter = {'resource_id': args[0]}

    combine(False, filter)
