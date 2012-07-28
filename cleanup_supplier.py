import requests
import json

import sqlaload as sl

from common import *

log = logging.getLogger('cleanup_supplier')
session = requests.session()

SUPPLIER_FIELDS = ['SupplierName']
SCORE_CUTOFF = 80

def lookup(val, engine):
    supplier_table = sl.get_table(engine, 'supplier')
    data = sl.find_one(engine, supplier_table, name=val)
    if data is not None:
        return data['canonical'], data['uri'], data['score']
    try:
        query = json.dumps({'query': val, 'limit': 1})
        res = session.get('http://opencorporates.com/reconcile',
                params={'query': query})
        data = {'name': val, 'canonical': None, 'uri': None, 'score': 0}
        if res.ok and res.json and len(res.json.get('result')):
            r = res.json.get('result').pop()
            data['canonical'] = r['name']
            data['uri'] = r['uri']
            data['score'] = r['score']
        log.info('OpenCorporates Lookup: %s -> %s', val, data['canonical'])
        sl.upsert(engine, supplier_table, data, unique=['name'])
        return data['canonical'], data['uri'], data['score']
    except Exception, ex:
        log.exception(ex)
        return None, None, None

def apply(row, engine):
    for field in SUPPLIER_FIELDS:
        val = row.get(field, '').strip()
        if not len(val):
            row[field + 'Canonical'] = None
            row[field + 'URI'] = None
        name, uri, score = lookup(val, engine)
        if name and score < SCORE_CUTOFF:
            name = None
            uri = None
        row[field + 'Canonical'] = name or val
        row[field + 'URI'] = uri
    return row
