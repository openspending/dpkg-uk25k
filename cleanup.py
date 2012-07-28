import sqlaload as sl

from common import *
from common import issue as _issue

import cleanup_dates
import cleanup_numbers
import cleanup_gov
import cleanup_supplier

log = logging.getLogger('cleanup')

def issue(engine, resource_id, resource_hash, message, data={}):
    _issue(engine, resource_id, resource_hash, 'cleanup',
           message, data=data)

def cleanup_sheet(engine, row, sheet_id):
    spending_table = sl.get_table(engine, 'spending')
    data = list(sl.find(engine, spending_table,
            resource_id=row['resource_id'],
            sheet_id=sheet_id))
    date_formats = cleanup_dates.detect_formats(data)
    if None in date_formats.values():
        log.warn("Couldn't detect date formats: %r", date_formats)
        issue(engine, row['resource_id'], row['retrieve_hash'],
              "Couldn't detect date formats", repr(date_formats))
        return False

    for row in data:
        row = cleanup_dates.apply(row, date_formats)
        row = cleanup_numbers.apply(row)
        row = cleanup_gov.apply(row)
        row = cleanup_supplier.apply(row, engine)
        sl.upsert(engine, spending_table, row,
                  unique=['id'])
    return True

def cleanup_resource(engine, source_table, row, force):
    if not row['combine_status']:
        return

    # Skip over tables we have already cleaned up
    if not force and sl.find_one(engine, source_table,
            resource_id=row['resource_id'],
            cleanup_status=True,
            cleanup_hash=row['combine_hash']) is not None:
        return

    log.info("Cleanup: %s, Resource %s", row['package_name'], row['resource_id'])

    status = True
    for sheet_id in range(0, row['sheets']):
        sheet_status = cleanup_sheet(engine, row, sheet_id)
        if status and not sheet_status:
            status = False
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'cleanup_hash': row['combine_hash'],
        'cleanup_status': status,
        }, unique=['resource_id'])

def cleanup_all(force=False):
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table):
        cleanup_resource(engine, source_table, row, force)

if __name__ == '__main__':
    cleanup_all(False)

