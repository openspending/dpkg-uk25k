import os, sys
import urlparse
import urllib
import hashlib
from datetime import datetime
from collections import defaultdict

import requests
import sqlaload as sl

from common import *
from common import issue as _issue
from functools import partial

log = logging.getLogger('retrieve')

def issue(engine, resource_id, resource_hash, message, data={}):
    _issue(engine, resource_id, resource_hash, 'retrieve',
           message, data=data)

def fix_url(url):
    # The correct character set for URLs is "broken". This is probably close enough.
    url = url.strip()
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

def calculate_hash(data):
    return hashlib.sha256(data).hexdigest()    

def retrieve(row, engine, source_table, force):
    content_id = None
    try:
        if force != 'download' and os.path.exists(source_path(row)):
            # cached file exists
            # See if it is in the db - db may have been
            # wiped since it was last downloaded
            record = sl.find_one(engine, source_table, resource_id=row['resource_id'])
            if force != 'hash' and record and record.get('retrieve_hash'):
                return 'Already cached and in database'
            # Calculate hash
            fh = open(source_path(row), 'rb')
            data = fh.read()
            fh.close()
            content_id = calculate_hash(data)
            success = True
            if force == 'hash':
                result = 'Already cached. Added hash to the database'
            else:
                result = 'Already cached but needed to add the hash to the database'                
        else:
            # need to fetch the file
            url = row['url']
            fixed_url = fix_url(url)
            url_printable = '"%s" fixed to "%s"' % (url, fixed_url) \
                            if fixed_url != url \
                            else url
            log.info('Fetching: %s, %s', row['package_name'], url_printable)
            res = requests.get(fixed_url, allow_redirects=True,
                               verify=False, timeout=30.0)
            success = res.ok
            if success:
                data = res.content
                content_id = calculate_hash(data)
                fh = open(source_path(row), 'wb')
                fh.write(data)
                fh.close()
                result = 'Downloaded'
            else:
                issue(engine, row['resource_id'], None,
                      'Download failed with bad HTTP status: %s' % res.status_code, url_printable)
                result = 'Download failed (status %s)' % res.status_code
    except requests.Timeout, re:
        result = 'Timeout accessing URL'
        issue(engine, row['resource_id'], None, 
              result, url_printable)
        success = False
    except Exception, re:
        log.exception(re)
        issue(engine, row['resource_id'], None, 
              'Exception occurred', unicode(re))
        success = False
        result = 'Exception occurred'
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'retrieve_status': success,
        'retrieve_hash': content_id},
        unique=['resource_id'])
    return result

def retrieve_all(force=False):
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    result_counts = defaultdict(int)
    for row in sl.all(engine, source_table):
        result = retrieve(row, engine, source_table, force)
        result_counts['total'] += 1
        result_counts[result] += 1
    log.info('Total %i URLs', result_counts.pop('total'))
    for result, count in result_counts.items():
        log.info('  %i %s', count, result)

def usage():
    usage = '''Usage: python %s [force-download|force-hash]
Where:
     'force-download' ignores the file cache and downloads all URLs anyway
     'force-hash' ignores any hashes previously stored for cached files
''' % sys.argv[0]
    print usage
    sys.exit(1)

if __name__ == '__main__':
    force = False
    if len(sys.argv) == 2:
        if sys.argv[1] in ('force', 'force-download'):
            force = 'download'
        elif sys.argv[1] == 'force-hash':
            force = 'hash'
        else:
            usage()
    elif len(sys.argv) > 2:
        usage()
    retrieve_all(force)

