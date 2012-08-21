import sqlaload as sl
import csv

from common import *

log = logging.getLogger('dump')

def convert_value(value):
    if value is None:
        value = u''
    elif not isinstance(value, unicode):
        value = unicode(value)
    return value.encode('utf-8')


def sources_metadata(engine):
    sources = {}
    log.info("Building sources index...")
    source_table = sl.get_table(engine, 'source')
    for source in sl.all(engine, source_table):
        data = {
            'SourceDatasetName': source.get('package_name'),
            'SourceDatasetID': source.get('package_id'),
            'SourceDatasetTitle': source.get('package_title'),
            'SourcePublisherName': source.get('publisher_name'),
            'SourcePublisherTitle': source.get('publisher_title'),
            'SourceID': source.get('resource_id'),
            'SourceURL': source.get('url'),
            'SourceFormat': source.get('format'),
            }
        sources[source['resource_id']] = data
    return sources

def generate_all():
    engine = db_connect()
    spending = sl.get_table(engine, 'spending')
    sources = sources_metadata(engine)
    signatures = set()
    for row in sl.find(engine, spending, valid=True):
        if row['signature'] in signatures:
            continue
        signatures.add(row['signature'])
        if not row['resource_id'] in sources:
            continue
        row.update(sources[row['resource_id']])
        row.pop('valid', True)
        row.pop('row_id', True)
        row.pop('resource_id', True)
        row.pop('resource_hash', True)
        row['RecordETLID'] = row.pop('id', None)
        row['RecordSignature'] = row.pop('signature', None)
        row['SourceSheetID'] = row.pop('sheet_id', None)
        yield row

def dump_all(filename):
    writer = None
    fh = open(filename, 'wb')
    for i, row in enumerate(generate_all()):
        if writer is None:
            writer = csv.DictWriter(fh, row.keys())
            writer.writeheader()
        writer.writerow({k: convert_value(v) for k,v in row.items()})
        if i % 1000 == 0:
            log.info("Writing: %s...", i)
    log.info("Finished: %s records exported.", i)
    fh.close()

if __name__ == '__main__':
    dump_all('spending.csv')

