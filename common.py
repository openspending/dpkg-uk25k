import os
import sys
import json
import logging
import datetime
import ConfigParser
from optparse import OptionParser

import nomenklatura
from ckanclient import CkanClient
from running_stats import OpenSpendingStats

logging.basicConfig(level=logging.NOTSET)
logging.getLogger('sqlaload').setLevel(level=logging.WARN)
logging.getLogger('requests').setLevel(level=logging.WARN)

log = logging.getLogger('common')

def issue(engine, resource_id, resource_hash, stage, message,
          data={}):
    import sqlaload as sl # this import is slow, so it is done inside this func
    table = sl.get_table(engine, 'issue')
    log = logging.getLogger('issue')
    log.debug("R[%s]: %s", resource_id, message)
    sl.add_row(engine, table, {
        'resource_id': resource_id,
        'resource_hash': resource_hash,
        'timestamp': datetime.datetime.utcnow(),
        'stage': stage,
        'message': message,
        'data': json.dumps(data)
        })

def clear_issues(engine, resource_id, stage):
    import sqlaload as sl # this import is slow, so it is done inside this func
    table = sl.get_table(engine, 'issue')
    sl.delete(engine, table,
              resource_id=resource_id,
              stage=stage,
    )

def source_path(row):
    source_dir = config_get('resource-cache.dir')
    if not os.path.isdir(source_dir):
        os.makedirs(source_dir)
    return os.path.join(source_dir, row['resource_id'])

config = None
def config_get(option):
    global config
    if not config:
        config = ConfigParser.ConfigParser()
        filename = config.read(['default.ini', 'config.ini'])
        assert filename, 'Could not find config.ini in CWD: %s' % os.getcwd()
    return config.get('uk25k', option)

def ckan_client():
    ckan_api = config_get('ckan-api.url')
    return CkanClient(base_location='http://data.gov.uk/api')

CONNECTION = []

def db_connect():
    if not len(CONNECTION):
        import sqlaload as sl
        sqlalchemy_url = config_get('sqlalchemy.url')
        log.info('Using database: %s', sqlalchemy_url)
        CONNECTION.append(sl.connect(sqlalchemy_url))
    return CONNECTION[0]

NK_DATASETS = {}

def nk_connect(dataset):
    if not dataset in NK_DATASETS:
        NK_DATASETS[dataset] = nomenklatura.Dataset(
                dataset,
                api_key='beaf2ff2-ea94-47c0-942f-1613a09056c2')
    return NK_DATASETS[dataset]

def parse_args(allow_row=False):
    filter_ = {}
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-f", "--force",
                      action="store_true", dest="force", default=False,
                      help="Don't skip previously processed records")
    if allow_row:
        parser.add_option("--row", type="int", dest="row")
    parser.add_option("-r", "--resource_id", dest="resource_id")
    parser.add_option("-d", "--dataset_name", dest="dataset_name")
    parser.add_option("-i", "--dataset_id", dest="dataset_id")
    parser.add_option("-p", "--publisher_name", dest="publisher_name")
    (options, args) = parser.parse_args()
    if args:
        print 'Error: there should be no args, just options'
        parser.print_help()
        sys.exit(1)
    if options.resource_id:
        filter_['resource_id'] = options.resource_id
    elif options.dataset_id:
        filter_['package_id'] = options.dataset_id
    elif options.dataset_name:
        filter_['package_name'] = options.dataset_name
    elif options.publisher_name:
        filter_['publisher_name'] = options.publisher_name
    return options, filter_
