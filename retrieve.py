import os, sys
import urlparse
import urllib
import requests
import sqlaload as sl
from datetime import datetime

from common import *
from functools import partial

log = logging.getLogger('retrieve')

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
        res = requests.get(fix_url(row['url']), allow_redirects=True,
                           verify=False, timeout=30.0)
        success = res.ok
        if success:
            data = res.raw.read()
            fh = open(source_path(row), 'wb')
            fh.write(data)
            fh.close()
            message = unicode(len(data))
        else:
            message = unicode(res.error)
    except requests.exceptions.RequestException, re:
        log.exception(re)
        message = unicode(re)
        success = False
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'retrieve_status': success,
        'retrieve_message': message},
        unique=['resource_id'])

def retrieve_all(force=False):
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.all(engine, source_table):
        retrieve(row, engine, source_table, force)

if __name__ == '__main__':
    retrieve_all()
