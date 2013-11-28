import os, sys
import urlparse
import urllib
import hashlib
from datetime import datetime
from collections import defaultdict
import httplib

import requests
import sqlaload as sl
import jinja2

from common import *
from functools import partial

STAGE = 'retrieve'
log = logging.getLogger(STAGE)

def fix_url(url):
    '''Try common corrections to URLs. They don't always work, so return a list
    of reasonable options to try.

    Fixable examples:
    * GBP 'pound sign' needs encoding to %C2%A3:
      * http://www.southwest.nhs.uk/freedomofinformation/pdf/Over%20%C2%A325,000%20expenditure%20Mth%208.csv

    Don't work when 'fixed':
    * Doesn't work encoding the '+' signs:
      * http://data.defra.gov.uk/GPC/Nov12-May13/Over+500+GPC+April+2013.csv
    * Second colon needs doesn't work when encoded:
      * http://webarchive.nationalarchives.gov.uk/20121025080026/http://decc.gov.uk/assets/decc/11/access-information/2169-departmental-spend-over-500--april-2011.csv
    '''
    # Always try the original URL first
    fixed_urls = [url]

    # Remove spaces at either end - a common error
    url = url.strip()
    if url not in fixed_urls:
        fixed_urls.append(url)

    # The correct character set for URLs is "broken". This is probably close enough.
    # DR: I don't fully understand - explain

    # URLs should be UTF8 encoded
    if isinstance(url, unicode):
        url = url.encode('utf-8', 'ignore')
    if url not in fixed_urls:
        fixed_urls.append(url)

    # Split and reassemble the URL to help with bad syntax,
    # fixing the encoding of the components as best we can.
    scheme, netloc, path, qs, anchor = urlparse.urlsplit(url)
    path = urllib.quote(path, '/%')
    url = urlparse.urlunsplit((scheme, netloc, path, qs, None))
    if url not in fixed_urls:
        fixed_urls.append(url)

    # other common problems
    if url.startswith('"'):
        url = url[1:]
    if not (url.lower().startswith('http://') or \
            url.lower().startswith('https://')):
        url = 'http://' + url
    if url not in fixed_urls:
        fixed_urls.append(url)

    return fixed_urls[1:] # don't include the original url

def calculate_hash(data):
    return hashlib.sha256(data).hexdigest()

def get_url(url):
    try:
        success = False
        res = requests.get(url, allow_redirects=True,
                           verify=False, timeout=30.0)
        if res.ok:
            success = True
            content_or_error = res.content
        else:
            content_or_error = 'Download failed with bad HTTP status %s' % res.status_code
    except requests.Timeout:
        content_or_error = 'Timeout accessing URL'
    except requests.exceptions.RequestException, e:
        content_or_error = e.__class__.__name__ # e.g. 'ConnectionError'
    except httplib.HTTPException, e:
        # reading the HTTP body can throw httplib.IncompleteRead
        content_or_error = e.__class__.__name__
    except requests.packages.urllib3.exceptions.LocationParseError, e:
        content_or_error = 'Location parse error'
    except Exception, e:
        log.exception(e)
        content_or_error = 'Exception occurred %r' % e.__class__.__name__
    return success, content_or_error

def retrieve(row, engine, source_table, force, stats):
    content_id = None
    if not force and row.get('retrieve_status') is True \
           and row.get('retrieve_hash') and os.path.exists(source_path(row)):
        # cached file exists and url is unchanged
        stats.add_source('Already cached and in database', row)
        return

    # fetch the file
    log.info("Retrieve: /dataset/%s/resource/%s", row['package_name'], row['resource_id'])
    clear_issues(engine, row['resource_id'], STAGE)
    url = row['url'].strip() # no-one can disagree with doing .strip()
    log.info('Fetching: "%s"', url)
    success, content_or_error = get_url(url)
    if not success:
        # URL didn't work, so try 'fixed' versions of it
        original_error = content_or_error
        fixed_urls = fix_url(url)
        for fixed_url in fixed_urls:
            log.info('Fetching fixed url: "%s"', fixed_url)
            success, content_or_error = get_url(fixed_url)
            if success:
                break
    if success:
        stats.add_source('Downloaded', row)
    elif os.path.exists(source_path(row)):
        stats.add_source('Could not download but it was in the cache', row)
        with open(source_path(row), 'rb') as fh:
            content_or_error = fh.read()
        success = True

    if success:
        data = content_or_error
        content_id = calculate_hash(data)
        fh = open(source_path(row), 'wb')
        fh.write(data)
        fh.close()
    else:
        stats.add_source(original_error, row)
        issue(engine, row['resource_id'], None, STAGE,
              original_error, url.encode('utf8', 'ignore'))
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
