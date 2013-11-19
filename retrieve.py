import os, sys
import urlparse
import urllib
import hashlib
from datetime import datetime
from collections import defaultdict

import requests
import sqlaload as sl

from common import *
from functools import partial

STAGE = 'retrieve'
log = logging.getLogger(STAGE)

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

def retrieve(row, engine, source_table, force, stats):
    content_id = None
    try:
        if force != 'download' and row.get('retrieve_status') == True \
               and row.get('retrieve_hash') and os.path.exists(source_path(row)):
            # cached file exists and url is unchanged
            stats.add_source('Already cached and in database', row)
            return
        else:
            # need to fetch the file
            clear_issues(engine, row['resource_id'], STAGE)
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
                stats.add_source('Downloaded', row)
            else:
                issue(engine, row['resource_id'], None, STAGE,
                      'Download failed with bad HTTP status: %s' % res.status_code, url_printable)
                stats.add_source('Download failed, HTTP status %s' % res.status_code, row)
    except requests.Timeout, re:
        result = 'Timeout accessing URL'
        stats.add_source(result, row)
        issue(engine, row['resource_id'], None, STAGE,
              result, url_printable)
        success = False
    except requests.exceptions.RequestException, e:
        result = e.__class__.__name__ # e.g. 'ConnectionError'
        stats.add_source(result, row)
        issue(engine, row['resource_id'], None, STAGE,
              result, url_printable)
        success = False
    except Exception, re:
        # Includes:
        # * httplib.IncompleteRead
        # * requests.packages.urllib3.exceptions.LocationParseError
        log.exception(re)
        issue(engine, row['resource_id'], None, STAGE,
              'Exception occurred', unicode(re))
        success = False
        stats.add_source('Exception occurred', row)
    sl.upsert(engine, source_table, {
        'resource_id': row['resource_id'],
        'retrieve_status': success,
        'retrieve_hash': content_id},
        unique=['resource_id'])

def retrieve_some(force=False, filter=None):
    stats = OpenSpendingStats()
    engine = db_connect()
    source_table = sl.get_table(engine, 'source')
    for row in sl.find(engine, source_table, **(filter or {})):
        retrieve(row, engine, source_table, force, stats)
    print 'Retrieve summary:'
    print stats.report()

def retrieve_all(force=False):
    retrieve_some(force=force)
   
if __name__ == '__main__':
    options, filter = parse_args()
    retrieve_some(force=options.force, filter=filter)
