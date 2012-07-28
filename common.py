import os
import json
import logging
import datetime
import sqlaload as sl
import nkclient as nk

logging.basicConfig(level=logging.NOTSET)
logging.getLogger('sqlaload').setLevel(level=logging.WARN)
logging.getLogger('requests').setLevel(level=logging.WARN)

log = logging.getLogger('issue')

def issue(engine, resource_id, resource_hash, stage, message,
          data={}):
    table = sl.get_table(engine, 'issue')
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
    source_dir = 'sources'
    if not os.path.isdir(source_dir):
        os.makedirs(source_dir)
    return os.path.join(source_dir, row['resource_id'])

def db_connect():
    return sl.connect("postgresql:///uk25k")

NK_DATASETS = {}

def nk_connect(dataset):
    if not dataset in NK_DATASETS:
        NK_DATASETS[dataset] = nk.NKDataset(
                dataset, 
                api_key='beaf2ff2-ea94-47c0-942f-1613a09056c2')
    return NK_DATASETS[dataset]


