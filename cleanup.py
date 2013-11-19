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

def cleanup_sheet(engine, row, sheet_id, stats_spending):
    spending_table = sl.get_table(engine, 'spending')
    data = list(sl.find(engine, spending_table,
            resource_id=row['resource_id'],
            sheet_id=sheet_id))
    connection = engine.connect()
    trans = connection.begin()
    date_formats = cleanup_dates.detect_formats(data)
    try:
        if None in date_formats.values():
            log.warn("Couldn't detect date formats: %r", date_formats)
            issue(engine, row['resource_id'], row['retrieve_hash'], STAGE,
                  "Couldn't detect date formats", repr(date_formats))
            return False, 'Couldn\'t detect date format'

        sl.delete(connection, spending_table,
                  resource_id=row['resource_id'],
                  sheet_id=sheet_id)
        for row in data:
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

def cleanup_resource(engine, source_table, row, force, stats, stats_spending):
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

    log.info("Cleanup: %s, Resource %s", row['package_name'], row['resource_id'])
    clear_issues(engine, row['resource_id'], STAGE)

    status = True
    error_message = None
    for sheet_id in range(0, row['sheets']):
        sheet_status, sheet_error_message = cleanup_sheet(engine, row, sheet_id, stats_spending)
        if status and not sheet_status:
            status = False
            error_message = sheet_error_message
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'cleanup_hash': row['combine_hash'],
        'cleanup_status': status,
        }, unique=['resource_id'])
    if status:
        stats.add_source('Cleaned up ok', row)
    else:
        stats.add_source(error_message, row)

def cleanup(force=False, filter=None):
    stats = OpenSpendingStats()
    stats_spending = defaultdict(OpenSpendingStats)
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table, **(filter or {})):
        cleanup_resource(engine, source_table, row, force, stats, stats_spending)
    log.info('Cleanup summary: \n%s' % stats.report())
    for key in stats_spending:
        log.info('Cleanup %s: \n%s' % (stat_type, stats_spending[key].report()))

if __name__ == '__main__':
    options, filter = parse_args()
    cleanup(force=options.force, filter=filter)

