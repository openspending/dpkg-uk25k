from common import *
import sqlaload as sl

import sys

from retrieve import retrieve
from extract import extract_resource
from combine import combine_resource
from cleanup import cleanup_resource
from validate import validate_resource

if __name__ == '__main__':
    DEBUG_RESOURCE = sys.argv[1]
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    row = sl.find_one(engine, source_table, resource_id=DEBUG_RESOURCE)
    retrieve(row, engine, source_table, force=True)
    row = sl.find_one(engine, source_table, resource_id=DEBUG_RESOURCE)
    extract_resource(engine, source_table, row, force=True)
    row = sl.find_one(engine, source_table, resource_id=DEBUG_RESOURCE)
    combine_resource(engine, source_table, row, force=True)
    row = sl.find_one(engine, source_table, resource_id=DEBUG_RESOURCE)
    cleanup_resource(engine, source_table, row, force=True)
    row = sl.find_one(engine, source_table, resource_id=DEBUG_RESOURCE)
    validate_resource(engine, source_table, row, force=True)

