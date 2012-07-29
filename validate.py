import sqlaload as sl
import hashlib

from common import *
from common import issue as _issue

log = logging.getLogger('validate')

def issue(engine, resource_id, resource_hash, message, data={}):
    _issue(engine, resource_id, resource_hash, 'validate',
           message, data=data)

def generate_signature(row):
    sig = '*$*'.join([
        unicode(row.get('AmountFormatted') or ''),
        row.get('DateFormatted') or '',
        row.get('SupplierNameCanonical') or '',
        row.get('EntityNameCanonical') or '',
        row.get('DepartmentFamilyNameCanonical') or '',
        row.get('ExpenseType') or '',
        row.get('ExpenseArea') or '',
        unicode(row.get('TransactionNumber') or '')
        ]).encode('utf-8')
    return unicode(hashlib.sha256(sig).hexdigest())


def validate_sheet(engine, row, sheet_id):
    spending_table = sl.get_table(engine, 'spending')
    data = list(sl.find(engine, spending_table,
            resource_id=row['resource_id'],
            sheet_id=sheet_id))

    records = 0
    for row in data:
        result = {'id': row['id'], 'valid': True}
        result['signature'] = generate_signature(row)

        if row['DateFormatted'] is None:
            result['valid'] = False
        if row['AmountFormatted'] is None:
            result['valid'] = False

        if result['valid']:
            records += 1
        sl.upsert(engine, spending_table, result,
                  unique=['id'])
    return records > 0

def validate_resource(engine, source_table, row, force):
    if not row['cleanup_status']:
        return

    # Skip over tables we have already cleaned up
    if not force and sl.find_one(engine, source_table,
            resource_id=row['resource_id'],
            validate_status=True,
            validate_hash=row['cleanup_hash']) is not None:
        return

    log.info("Validate: %s, Resource %s", row['package_name'], row['resource_id'])

    status = True
    for sheet_id in range(0, row['sheets']):
        sheet_status = validate_sheet(engine, row, sheet_id)
        if status and not sheet_status:
            status = False
    log.info("Result: %s", status)
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'validate_hash': row['cleanup_hash'],
        'validate_status': status,
        }, unique=['resource_id'])

def validate_all(force=False):
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table):
        validate_resource(engine, source_table, row, force)

if __name__ == '__main__':
    validate_all(False)

