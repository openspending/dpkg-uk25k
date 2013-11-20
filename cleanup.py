import sys
from collections import defaultdict

import sqlaload as sl

from common import *

import cleanup_dates
import cleanup_numbers
import cleanup_gov
import cleanup_supplier

STAGE = 'cleanup'
log = logging.getLogger(STAGE)

def cleanup_sheet(engine, row, sheet_id, data_row_filter, stats_spending):
    spending_table = sl.get_table(engine, 'spending')
    data = list(sl.find(engine, spending_table,
            resource_id=row['resource_id'],
            sheet_id=sheet_id))
    if not data:
        log.info('Sheet has no rows')
        return False, None
    connection = engine.connect()
    trans = connection.begin()
    date_formats = cleanup_dates.detect_formats(data)
    try:
        for date_format in date_formats.values():
            if isinstance(date_format, basestring):
                issue(engine, row['resource_id'], row['retrieve_hash'], STAGE,
                        "Couldn't detect date formats because: %s" % date_format,
                        repr(date_formats))
                return True, date_format

        if not data_row_filter:
            sl.delete(connection, spending_table,
                      resource_id=row['resource_id'],
                      sheet_id=sheet_id)
        for row in data:
            if data_row_filter and data_row_filter != row['row_id']:
                continue
            row = cleanup_dates.apply(row, date_formats, stats_spending)
            row = cleanup_numbers.apply(row, stats_spending)
            row = cleanup_gov.apply(row, stats_spending)
            #row = cleanup_supplier.apply(row, engine)
            del row['id']
            sl.add_row(connection, spending_table, row)
        trans.commit()
        return True, None
    finally:
        connection.close()

def cleanup_resource(engine, source_table, row, force, data_row_filter, stats, stats_spending):
    if not row['combine_status']:
        stats.add_source('Previous step (combine) not complete', row)
        return

    # Skip over tables we have already cleaned up
    if not force and sl.find_one(engine, source_table,
            resource_id=row['resource_id'],
            cleanup_status=True,
            cleanup_hash=row['combine_hash']) is not None:
        stats.add_source('Already cleaned up', row)
        return

    log.info("Cleanup: /dataset/%s/resource/%s", row['package_name'], row['resource_id'])
    if not data_row_filter:
        clear_issues(engine, row['resource_id'], STAGE)

    no_rows = True
    no_errors = True
    error_message = None
    for sheet_id in range(0, row['sheets']):
        sheet_has_rows, sheet_error_message = cleanup_sheet(engine, row, sheet_id, data_row_filter, stats_spending)
        if no_errors and sheet_error_message:
            no_errors = False
            error_message = sheet_error_message
        if no_rows and sheet_has_rows:
            no_rows = False
    if data_row_filter:
        stats.add_source('Resource data filtered, not saving resource cleanup.', row)
    else:
        sl.upsert(engine, source_table, {
            'resource_id': row['resource_id'],
            'cleanup_hash': row['combine_hash'],
            'cleanup_status': no_errors,
            }, unique=['resource_id'])
        if no_rows:
            stats.add_source('Empty sheet', row)
        elif no_errors:
            stats.add_source('Cleaned up ok', row)
        else:
            stats.add_source(error_message, row)

def cleanup(force=False, resource_filter=None, data_row_filter=None):
    stats = OpenSpendingStats()
    stats_spending = defaultdict(OpenSpendingStats)
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table, **(filter or {})):
        cleanup_resource(engine, source_table, row, force, data_row_filter, stats, stats_spending)
    log.info('Cleanup summary: \n%s' % stats.report())
    for key in stats_spending:
        log.info('Cleanup %s: \n%s' % (key, stats_spending[key].report()))

if __name__ == '__main__':
    options, filter = parse_args(allow_row=True)
    cleanup(force=options.force, resource_filter=filter, data_row_filter=options.row)

