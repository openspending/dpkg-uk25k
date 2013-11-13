import sys

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


def validate_sheet(engine, row, sheet_id, stats_spending):
    spending_table = sl.get_table(engine, 'spending')
    data = list(sl.find(engine, spending_table,
            resource_id=row['resource_id'],
            sheet_id=sheet_id))
    connection = engine.connect()
    trans = connection.begin()
    issue_noted_for_this_resource = False # record first failure only
    try:
        records = 0
        for row_ in data:
            result = {'id': row_['id'], 'valid': True}
            result['signature'] = generate_signature(row_)

            if row_['DateFormatted'] is None:
                stats_spending['date'].add_spending('Date invalid', row_)
                result['valid'] = False
                if not issue_noted_for_this_resource:
                    issue(engine, row['resource_id'], row['retrieve_hash'],
                          'Date invalid (or possible the date format is inconsistent)',
                          {'row_id': row_.get('row_id'),
                           'Date': row_.get('Date')})
                    issue_noted_for_this_resource = True
            else:
                stats_spending['date'].add_spending('Date ok', row_)

            if row_['AmountFormatted'] is None:
                stats_spending['amount'].add_spending('Amount invalid', row_)
                result['valid'] = False
                if not issue_noted_for_this_resource:
                    issue(engine, row['resource_id'], row['retrieve_hash'],
                          'Amount invalid', {'row_id': row_.get('row_id'),
                                             'Amount': row_.get('Amount')})
                    issue_noted_for_this_resource = True
            else:
                stats_spending['amount'].add_spending('Amount ok', row_)

            if result['valid']:
                records += 1
            sl.update(connection, spending_table,
                      {'id': result['id']}, result)
        trans.commit()
        return records > 0
    finally:
        connection.close()

def validate_resource(engine, source_table, row, force, stats, stats_spending):
    if not row['cleanup_status']:
        stats.add_source('Previous step (cleanup) not complete', row)
        return

    # Skip over tables we have already cleaned up
    if not force and sl.find_one(engine, source_table,
            resource_id=row['resource_id'],
            validate_status=True,
            validate_hash=row['cleanup_hash']) is not None:
        stats.add_source('Already validated', row)
        return

    log.info("Validate: %s, Resource %s", row['package_name'], row['resource_id'])

    status = True
    for sheet_id in range(0, row['sheets']):
        sheet_status = validate_sheet(engine, row, sheet_id, stats_spending)
        if status and not sheet_status:
            status = False
    log.info("Result: %s", status)
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'validate_hash': row['cleanup_hash'],
        'validate_status': status,
        }, unique=['resource_id'])
    if status:
        stats.add_source('Validated ok', row)
    else:
        stats.add_source(error_message, row)

def validate(force=False, filter=None):
    stats = OpenSpendingStats()
    stats_spending = {'date': OpenSpendingStats(),
                      'amount': OpenSpendingStats()}
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table, **(filter or {})):
        validate_resource(engine, source_table, row, force, stats, stats_spending)
    log.info('Validate summary: \n%s' % stats.report())
    for stat_type in stats_spending:
        log.info('Validate %s: \n%s' % (stat_type, stats_spending[stat_type].report()))

if __name__ == '__main__':
    args = sys.argv[1:]
    filter = {}
    force = False
    if '-h' in args or '--help' in args or len(args) > 2:
        print 'Usage: python %s [<resource ID>]' % sys.argv[0]
        sys.exit(1)
    elif len(args) == 1:
        filter = {'resource_id': args[0]}
        force = True

    validate(force=force, filter=filter)

