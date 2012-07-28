import os, sys
import urlparse
import urllib
import hashlib
import requests
import sqlaload as sl
from datetime import datetime

from common import *
from common import issue as _issue
from functools import partial

log = logging.getLogger('retrieve')

def issue(engine, resource_id, resource_hash, message, data={}):
    _issue(engine, resource_id, resource_hash, 'retrieve',
           message, data=data)

def fix_url(url):
    # The correct character set for URLs is "broken". This is probably close enough.
    if isinstance(url, unicode):
        url = url.encode('utf-8', 'ignore')
    scheme, netloc, path, qs, anchor = urlparse.urlsplit(url)
    path = urllib.quote(path, '/%')
    url = urlparse.urlunsplit((scheme, netloc, path, qs, None))
    #url = url.replace(" ", "%20")
    if url.startswith('"'):
        url = url[1:]
    if not (url.lower().startswith('http://') or \
            url.lower().startswith('https://')):
        url = 'http://' + url
    return url

def retrieve(row, engine, source_table, force):
    if not force and os.path.exists(source_path(row)):
        return
    log.info('Fetching: %s, %s', row['package_name'], row['url'])
    try:
        content_id = None
        res = requests.get(fix_url(row['url']), allow_redirects=True,
                           verify=False, timeout=30.0)
        success = res.ok
        if success:
            data = res.raw.read()
            content_id = hashlib.sha256(data).hexdigest()
            fh = open(source_path(row), 'wb')
            fh.write(data)
            fh.close()
        else:
            issue(engine, row['resource_id'], None,
                  str(res.status_code), data=res.content)
    except Exception, re:
        log.exception(re)
        issue(engine, row['resource_id'], None, 
              unicode(re))
        success = False
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'retrieve_status': success,
        'retrieve_hash': content_id},
        unique=['resource_id'])

def retrieve_all(force=False):
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.all(engine, source_table):
        retrieve(row, engine, source_table, force)

if __name__ == '__main__':
    retrieve_all()
