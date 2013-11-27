from pprint import pprint
import sqlaload as sl
import os
import sys

from jinja2 import FileSystemLoader, Environment
from common import *

log = logging.getLogger('report')
templates = FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates'))

# Jinja filters setup

env = Environment(loader=templates)

def percentage(num, base=1):
    n = (float(num)/float(base)) * 100
    return str(int(n)) + '%'

env.filters['pct'] = percentage

def british_date(value):
    if not value:
        return ''
    y, m, d = value.split('-')
    return '%s/%s/%s' % (d, m, y)
env.filters['british_date'] = british_date

def currency_format(value):
    ''' 12345.67 -> 12,345 '''
    if not value:
        return ''
    try:
        # Python 2.7
        return '{:,.0f}'.format(value)
    except ValueError:
        # Python 2.6 - cannot do commas
        return '{0:.0f}'.format(value)
env.filters['currency_format'] = currency_format

stage_name_mapping = {
    'retrieve': 'Download',
    'extract': 'Format',
    'combine': 'Columns',
    'cleanup': 'Data',
    'validate': 'Valid',
    }
def stage_name_map(stage_name):
    '''Maps what the stage name from what this software calls it
    to the name the user sees.'''
    return stage_name_mapping[stage_name]
env.filters['stage_name_map'] = stage_name_map

stage_descriptions = {
    'retrieve': 'Download the data file using the URL',
    'extract': 'Opens the data file, checks the format of it matches XLS or CSV and it reads the raw transaction data',
    'combine': 'Column titles are normalised and any not recognised are flagged up',
    'cleanup': 'Dates are parsed, numbers parsed and supplier names are normalised as much as possible',
    'validate': 'Checks each transaction has a valid date and amount. Those that don\'t are discarded',
    }
def stage_description_map(stage_name):
    '''Provides a decription of a stage'''
    return stage_descriptions[stage_name]
env.filters['stage_description_map'] = stage_description_map

def write_report(dest_dir, template, name, **kw):
    template = env.get_template(template)
    report_ts = datetime.datetime.utcnow().strftime("%B %d, %Y")
    report = template.render(report_ts=report_ts, **kw)
    filepath = os.path.join(dest_dir, name + '.html')
    with open(filepath, 'wb') as fh:
        fh.write(report.encode('utf-8'))
    return filepath

def all_groups():
    client = ckan_client()
    for group_name in client.group_register_get():
        yield get_group(group_name)

def get_group(group_name):
    client = ckan_client()
    group = client.group_entity_get(group_name)
    group['spending_published_by'] = group.get('extras').get('spending_published_by')
    group['category'] = group.get('extras').get('category')
    group['must_report'] = group['category'] in ['core-department']
    return group

def group_query(engine):
    stats = {}
    q = """
        SELECT
            src.publisher_name AS name,
            MAX(src.last_modified) AS last_modified,
            COUNT(DISTINCT src.id) AS num_sources,
            COUNT(spe.id) AS num_entries,
            SUM(spe."AmountFormatted"::float) AS total,
            MAX(spe."DateFormatted") AS latest,
            MIN(spe."DateFormatted") AS oldest
        FROM source src LEFT OUTER JOIN spending spe
            ON spe.resource_id = src.resource_id
            AND spe.valid = true
        GROUP BY src.publisher_name;
        """
    r = engine.execute(q)
    for res in sl.resultiter(r):
        res['top_class'] = False
        if res['latest']:
            dt = datetime.datetime.strptime(res['latest'], "%Y-%m-%d")
            ref = datetime.datetime.now() - datetime.timedelta(days=62)
            res['top_class'] = dt > ref
        stats[res['name']] = res
    return stats

def group_data(engine, publisher_filter):
    '''Gets each group from CKAN as a dictionary, adds in the
    stats for it and yields it.'''
    stats = group_query(engine)
    if publisher_filter:
        groups = [get_group(group_str) for group_str in publisher_filter]
    else:
        groups = all_groups()
    for i, group in enumerate(groups):
        group.update(stats.get(group.get('name'), {}))
        print group['name']
        yield group
        #if i > 20:
        #    break

def publisher_report(engine, dest_dir, publisher_filter):
    '''Creates a report of all the publishers and their overall spending results'''
    _all_groups = list(group_data(engine, publisher_filter))
    groups_by_name = dict([(g['name'], g) for g in _all_groups])
    by_names = dict([(g['name'], g['title']) for g in _all_groups])

    # when a group's transactions are published by another group,
    # copy the results into it
    published_by_other = filter(lambda g: g.get('spending_published_by', 0), _all_groups)
    for group in published_by_other:
        publishing_group = group['spending_published_by']
        assert isinstance(publishing_group, basestring), publishing_group
        publishing_group = groups_by_name[publishing_group]
        for property in ('num_entries', 'num_sources', 'top_class'):
            if property in publishing_group:
                group[property] = publishing_group[property]

    req_groups = filter(lambda g: g['must_report'], _all_groups)
    nonreq_groups = filter(lambda g: not g['must_report'], _all_groups)
    num = len(req_groups)
    #shows = filter(lambda g: g.get('num_sources', 0) > 0, req_groups)
    valids = filter(lambda g: g.get('num_entries', 0) > 0, req_groups)
    top_class_groups = filter(lambda g: g.get('top_class'), req_groups)

    def within(groups, field, format_, **kw):
        '''Returns groups filtered by whether it has an entry with a field value recent enough.'''
        def _wi(g):
            dt = g.get(field)
            if not dt:
                return False
            dt = dt.rsplit('.', 1)[0]
            dt = datetime.datetime.strptime(dt, format_)
            ref = datetime.datetime.now() - datetime.timedelta(**kw)
            return dt > ref
        return filter(_wi, groups)

    def within_m(groups, **kw):
        '''Of the groups given, return those with valid spending metadata in a recent period.

        :param kw: the time period (timedelta arguments)
        '''
        return within(groups, 'last_modified', '%Y-%m-%dT%H:%M:%S', **kw)

    def within_c(groups, **kw):
        '''Of the groups given, return those which have entries in a recent period.

        :param kw: the time period (timedelta arguments)
        '''
        return within(groups, 'latest', '%Y-%m-%d', **kw)

    stats = {
        'num': len(req_groups),
        'numf': float(len(req_groups)),
#        'reported_ever': len(shows),
#        'reported_3m': len(within_m(shows, weeks=12)),
#        'reported_6m': len(within_m(shows, weeks=26)),
#        'reported_1y': len(within_m(shows, weeks=52)),
#        'valid_ever': len(valids),
#        'valid_3m': len(within_m(valids, weeks=12)),
#        'valid_6m': len(within_m(valids, weeks=26)),
#        'valid_1y': len(within_m(valids, weeks=52)),
        'cover_ever': len(valids),
        'cover_2m': len(top_class_groups),
#        'cover_2m': len(within_c(valids, days=62)),
#        'cover_3m': len(within_c(valids, weeks=12)),
#        'cover_6m': len(within_c(valids, weeks=26)),
#        'cover_1y': len(within_c(valids, weeks=52)),
        }
    pprint(stats)
    filepath = write_report(dest_dir, 'publishers.html',
            'index', req_groups=req_groups,
            all_groups=_all_groups,
            by_names=by_names,
            nonreq_groups=nonreq_groups,
            stats=stats)
    log.info('Wrote publisher report: %s', filepath)

def resource_query(engine):
    data = {}
    q = """
        SELECT
            src.*,
            COUNT(spe.id) AS num_entries,
            SUM(spe."AmountFormatted"::float) AS total,
            MAX(spe."DateFormatted") AS latest,
            MIN(spe."DateFormatted") AS oldest
        FROM source src LEFT OUTER JOIN spending spe
            ON src.resource_id = spe.resource_id
            AND spe.valid = true
        GROUP BY src.id
        """
    r = engine.execute(q)
    for res in sl.resultiter(r):
        issues = list(sl.resultiter(engine.execute(
                """ SELECT message, data, stage FROM issue WHERE resource_id = '%s' AND stage = 'retrieve'
                ORDER BY timestamp DESC """ % (res['resource_id']))))
        issues += list(sl.resultiter(engine.execute(
                """ SELECT message, data, stage FROM issue WHERE resource_id = '%s' AND resource_hash = '%s'
                ORDER BY timestamp DESC """ % (res['resource_id'], res['retrieve_hash']))))
        issues = set([(i['stage'], i['message'], i['data']) for i in issues])
        res['issues'] = issues
        pn = res['publisher_name']
        if pn is None:
            continue
        if not pn in data:
            data[pn] = []
        data[pn].append(res)
    return data

def resource_report(engine, dest_dir, publisher_filter=None):
    '''For each publisher it creates a report of each of its resources and how
    they fared in the ETL.
    '''
    data = resource_query(engine)
    publisher_names = publisher_filter or data
    for publisher_name in publisher_names:
        if publisher_name not in data:
            log.info('No data for publisher: %s', publisher_name)
            continue
        resources = data[publisher_name]
        filepath = write_report(dest_dir, 'resources.html',
            'publisher-' + publisher_name,
            resources=resources,
            publisher_name=publisher_name,
            publisher_title=resources[0].get('publisher_title'))
        log.info('Wrote resource report for %s: %s', publisher_name, filepath)

def create_report(dest_dir, publisher_filter, report_name):
    if not os.path.isdir(dest_dir):
        os.makedirs(dest_dir)
    engine = db_connect()
    if report_name == 'all':
        publisher_report(engine, dest_dir, publisher_filter)
        resource_report(engine, dest_dir, publisher_filter)
    elif report_name == 'publisher':
        publisher_report(engine, dest_dir, publisher_filter)
    elif report_name == 'resource':
        resource_report(engine, dest_dir, publisher_filter)

REPORT_NAMES = ('publisher', 'resource')

if __name__ == '__main__':
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-p", "--publisher_name", dest="publisher_name",
            help="Publisher name (or multiple ones comma-separated)")
    parser.add_option("-o", "--output_dir", dest="output_dir",
            help="Directory to write the reports to (defaults to config "
            "option: report.output.dir)")
    parser.add_option("--report", dest="report", default="all",
            help='Report, chosen from: all %s' % ' '.join(REPORT_NAMES))
    (options, args) = parser.parse_args()
    if args:
        parser.error('there should be no args, just options')
    if options.publisher_name:
        publisher_filter = options.publisher_name.split(',')
    else:
        publisher_filter = None
    try:
        output_dir = config_get('report.output.dir')
    except ConfigParser.NoOptionError:
        output_dir = None
    if not output_dir:
        output_dir = options.output_dir
    if not output_dir:
        parser.error('need to specify an output directory')
    if options.report is not 'all' and options.report not in REPORT_NAMES:
        parser.error('report name must be "all" or one of: %r' % REPORT_NAMES)
    log.info('Report output dir: %s' % output_dir)
    create_report(output_dir, publisher_filter, options.report)
