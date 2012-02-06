import urlparse
import urllib
import requests
import sqlaload as sl
from datetime import datetime

from common import source_path

def fix_url(url):
    url = url.replace(" ", "%20")
    if url.startswith('"'):
        print "FOO"
        url = url[1:]
    _url = url.lower()
    if not _url.startswith('http://') or _url.startswith('https://'):
        url = 'http://' + url
    return url

def retrieve(row, engine):
    ret_table = sl.get_table(engine, 'retrieval_log')
    #print row.get('package_name'), row['url'].encode('utf-8')
    try:
        import os
        if os.path.exists(source_path(row)):
            return
        res = requests.get(fix_url(row['url']))
        fh = open(source_path(row), 'wb')
        fh.write(res.raw.read())
        fh.close()
        #url = urlparse.urlparse(row['url'])
        #url = [urllib.quote(p) if i!=1 else p for i, p in enumerate(url)]
        #url = urlparse.urlunparse(url)
        #res = urllib.urlretrieve(url, source_path(row))
        sl.add_row(engine, ret_table, {
            'resource_id': row['resource_id'],
            'status': res.status_code,
            'message': "",
            'timestamp': datetime.now()
            })
    except Exception, ioe:
        status = ioe.status if hasattr(ioe, 'status') else ""
        sl.add_row(engine, ret_table, {
            'resource_id': row['resource_id'],
            'status': status,
            'message': unicode(ioe),
            'timestamp': datetime.now()
            })
        assert False, unicode(ioe).encode('utf-8')


def test_retrieve_all():
    engine = sl.connect("sqlite:///uk25k.db")
    src_table = sl.get_table(engine, 'source')
    for row in sl.all(engine, src_table):
        retrieve.description = 'retrieve: %(package_name)s/%(resource_id)s (%(url)s)' % row
        yield retrieve, row, engine


#if __name__ == '__main__':
#    retrieve_all(engine)
