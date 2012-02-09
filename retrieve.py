import urlparse
import urllib
import urllib2
import sqlaload as sl
import sys
from datetime import datetime
import traceback

from common import *
from functools import partial

binary_formats = ['.xls', 'xls', 'xlx', 'xlsx', 'zip', 'pdf', 'Zipped CSV', 'Excel']

def fix_url(url):
    # The correct character set for URLs is "broken". This is probably close enough.
    if isinstance(url, unicode):
        url = url.encode('utf-8', 'ignore')
    scheme, netloc, path, qs, anchor = urlparse.urlsplit(url)
    path = urllib.quote(path, '/%')
    url = urlparse.urlunsplit((scheme, netloc, path, qs, anchor))

    url = url.replace(" ", "%20")
    if url.startswith('"'):
        print "FOO"
        url = url[1:]
    _url = url.lower()
    if not (_url.startswith('http://') or _url.startswith('https://')):
        url = 'http://' + url
    return url

def retrieve(row, engine, force):
    ret_table = sl.get_table(engine, 'retrieval_log')
    #print row.get('package_name'), row['url'].encode('utf-8')
    try:
        import os
        if not force and os.path.exists(source_path(row)):
            return
        url = fix_url(row['url'])
        print "Fetching %s" % url
        res = urllib2.urlopen(url)

        fh = open(source_path(row), 'wb')
        fh.write(res.read())

        sl.add_row(engine, ret_table, {
            'resource_id': row['resource_id'],
            'status': '200',
            'message': "",
            'content-type': res.headers.get('content-type', ''),
            'timestamp': datetime.now()
            })
    except Exception, ioe:
        print traceback.format_exc()
        status = 0
        if hasattr(ioe, 'code'):
            status = ioe.code
        sl.add_row(engine, ret_table, {
            'resource_id': row['resource_id'],
            'status': status,
            'message': unicode(ioe),
            'timestamp': datetime.now()
            })
        assert False, unicode(ioe).encode('utf-8')

def connect():
    engine = db_connect()
    src_table = sl.get_table(engine, 'source')
    return engine,src_table

def describe(row):
    return 'retrieve: %(package_name)s/%(resource_id)s (%(url)s)' % row

def test_retrieve_all():
    engine,src_table = connect()
    for row in sl.all(engine, src_table):
        f = partial(retrieve, row, engine, False)
        f.description = describe(row)
        yield f,

if __name__ == '__main__':
    engine,src_table = connect()
    for id in sys.argv[1:]:
        row = sl.find_one(engine, src_table, resource_id=id)
        if row is None:
            print "Could not find row %s" % id
        else:
            print describe(row)
            retrieve(row, engine, True)
