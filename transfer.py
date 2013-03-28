'''Transfer a transaction database dump to the OpenSpending website where
it can be browsed and searched.

The spending.csv that is the result of dump.py is an optional parameter.

Requires config variables set to appropriate values:
  openspending.apikey
  openspending-dataset-source.url
'''

import sys
import os
import requests

from common import *

def transfer(spend_data_filepath):
    # gather all the options
    if not os.path.exists(spend_data_filepath):
        print 'Error: Spend data file does not exist: %r' % spend_data_filepath
        sys.exit(1)
    size = os.stat(spend_data_filepath).st_size
    if size < 3e9:
        print 'Error: Size of the spend data (%i bytes) seems too small to be a complete DGU spend database!' % size
        sys.exit(1)
    apikey = config_get('openspending.apikey')
    if not apikey:
        print 'Error: Essential config option is not specified: openspending.apikey'
        sys.exit(1)
    url = config_get('openspending-dataset-source.url')
    if not url:
        print 'Error: Essential config option is not specified: openspending-dataset-source.url'
        sys.exit(1)

    # do the transfer
    post_url = url + '/load'
    headers = {'Authorization': 'Apikey %s' % apikey}
    print 'Transferring %s (%.1f GB) to %s' % (spend_data_filepath, float(size)/1e9, post_url)
    with open(spend_data_filepath, 'rb') as f:
        res = requests.post(post_url, data=f, headers=headers)
    if not res.ok:
        print 'Error: POSTing the data did not succeed: %s %s' % \
              (res.status_code, res.content)
        sys.exit(1)
    print 'Transferred ok' % spend_data_filepath

if __name__ == '__main__':
    args = sys.argv[1:]
    filter = {}
    if '-h' in args or '--help' in args or len(args) > 1:
        print __doc__
        print 'Usage: python %s [<spending.csv>]' % sys.argv[0]
        sys.exit(1)
    elif len(args) == 1:
        spend_data_filepath = args[0]
    elif len(args) == 0:
        spend_data_filepath = 'spending.csv'

    transfer(spend_data_filepath)
