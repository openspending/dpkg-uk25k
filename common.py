import os
import json
import logging
import datetime
import ConfigParser
import sqlaload as sl
import nomenklatura
from ckanclient import CkanClient
from running_stats import OpenSpendingStats

logging.basicConfig(level=logging.NOTSET)
logging.getLogger('sqlaload').setLevel(level=logging.WARN)
logging.getLogger('requests').setLevel(level=logging.WARN)

log = logging.getLogger('common')

def issue(engine, resource_id, resource_hash, stage, message,
          data={}):
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
        config.read(['default.ini', 'config.ini'])
    return config.get('uk25k', option)

def ckan_client():
    ckan_api = config_get('ckan-api.url')
    return CkanClient(base_location='http://data.gov.uk/api')

CONNECTION = []

def db_connect():
    if not len(CONNECTION):
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


