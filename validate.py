import sys

import sqlaload as sl
import hashlib

from common import *

STAGE = 'validate'
log = logging.getLogger(STAGE)

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


def validate_sheet(engine, row, sheet_id, data_row_filter, stats_spending):
    spending_table = sl.get_table(engine, 'spending')
    data = list(sl.find(engine, spending_table,
            resource_id=row['resource_id'],
            sheet_id=sheet_id))
    connection = engine.connect()
    trans = connection.begin()
    issue_noted_for_this_resource = False # record first failure only
    error_message = None
    try:
        records = 0
        for row_ in data:
            if data_row_filter and data_row_filter != row_['row_id']:
                continue
            result = {'id': row_['id'], 'valid': True}
            result['signature'] = generate_signature(row_)

            if row_['DateFormatted'] is None:
                stats_spending['date'].add_spending('Date invalid', row_)
                result['valid'] = False
                if not issue_noted_for_this_resource:
                    issue(engine, row['resource_id'], row['retrieve_hash'], STAGE,
                          'Date invalid (or possible the date format is inconsistent)',
                          {'row_id': row_.get('row_id'),
                           'row_number': row_.get('row_number'),
                           'Date': row_.get('Date')})
                    error_message = 'Date invalid'
                    issue_noted_for_this_resource = True
            else:
                stats_spending['date'].add_spending('Date ok', row_)

            if row_['AmountFormatted'] is None:
                stats_spending['amount'].add_spending('Amount invalid', row_)
                result['valid'] = False
                if not issue_noted_for_this_resource:
                    issue(engine, row['resource_id'], row['retrieve_hash'], STAGE,
                          'Amount invalid', {'row_id': row_.get('row_id'),
                                             'row_number': row_.get('row_number'),
                                             'Amount': row_.get('Amount')})
                    error_message = 'Amount invalid'
                    issue_noted_for_this_resource = True
            else:
                stats_spending['amount'].add_spending('Amount ok', row_)

            if result['valid']:
                records += 1
            sl.update(connection, spending_table,
                      {'id': result['id']}, result)
        trans.commit()
        return records > 0, error_message
    finally:
        connection.close()

def validate_resource(engine, source_table, row, force, data_row_filter, stats, stats_spending):
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

    log.info("Validate: /dataset/%s/resource/%s", row['package_name'], row['resource_id'])
    if not data_row_filter:
        clear_issues(engine, row['resource_id'], STAGE)

    no_errors = True
    no_records = True
    error_message = None
    for sheet_id in range(0, row['sheets']):
        sheet_records, sheet_error_message = validate_sheet(engine, row, sheet_id, data_row_filter, stats_spending)
        if no_errors and sheet_error_message:
            no_errors = False
            error_message = sheet_error_message
        if no_records and sheet_records:
            no_records = False
    
    if data_row_filter:
        stats.add_source('Resource data filtered, not saving resource cleanup.', row)
    else:
        log.info("Result: records=%s errors=%s", not no_records, not no_errors)
        sl.upsert(engine, source_table, {
            'resource_id': row['resource_id'],
            'validate_hash': row['cleanup_hash'],
            'validate_status': no_errors,
            }, unique=['resource_id'])
        if no_errors:
            if no_records:
                stats.add_source('No records but no errors', row)
            else:
                stats.add_source('Validated ok', row)
        else:
            if no_records:
                stats.add_source('All transactions invalid: %s' % error_message, row)
            else:
                stats.add_source('Some transactions invalid: %s' % error_message, row)

def validate(force=False, filter=None, data_row_filter=None):
    stats = OpenSpendingStats()
    stats_spending = {'date': OpenSpendingStats(),
                      'amount': OpenSpendingStats()}
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table, **(filter or {})):
        validate_resource(engine, source_table, row, force, data_row_filter, stats, stats_spending)
    log.info('Validate summary: \n%s' % stats.report())
    for stat_type in stats_spending:
        log.info('Validate %s: \n%s' % (stat_type, stats_spending[stat_type].report()))

if __name__ == '__main__':
    options, filter = parse_args(allow_row=True)
    validate(force=options.force, filter=filter, data_row_filter=options.row)

