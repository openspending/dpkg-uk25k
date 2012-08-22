from pprint import pprint
import sqlaload as sl
import os

from jinja2 import FileSystemLoader, Environment
from ckanclient import CkanClient
from common import *

log = logging.getLogger('report')
templates = FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates'))
env = Environment(loader=templates)

def percentage(num, base=1):
    n = (float(num)/float(base)) * 100
    return str(int(n)) + '%'

env.filters['pct'] = percentage

def write_report(dest_dir, template, name, **kw):
    template = env.get_template(template)
    report = template.render(**kw)
    with open(os.path.join(dest_dir, name + '.html'), 'wb') as fh:
        fh.write(report.encode('utf-8'))

def all_groups():
    client = CkanClient(base_location='http://data.gov.uk/api')
    for name in client.group_register_get():
        group = client.group_entity_get(name)
        #group['type']
        yield group

def group_query(engine):
    stats = {}
    q = """
        SELECT
            src.publisher_name AS name,
            MAX(src.last_modified) AS last_modified,
            COUNT(DISTINCT src.id) AS num_sources,
            COUNT(spe.id) AS num_entries,
            SUM(spe."AmountFormatted") AS total,
            MAX(spe."DateFormatted") AS latest,
            MIN(spe."DateFormatted") AS oldest
        FROM source src LEFT OUTER JOIN spending spe
            ON spe.resource_id = src.resource_id
            AND spe.valid = true
        GROUP BY src.publisher_name;
        """
    r = engine.execute(q)
    for res in sl.resultiter(r):
        stats[res['name']] = res
    r = engine.execute(q)
    for res in sl.resultiter(r):
        stats[res['name']].update(res)
    return stats

def group_data(engine):
    stats = group_query(engine)
    for i, group in enumerate(all_groups()):
        group.update(stats.get(group.get('name'), {}))
        print [group['title']]
        yield group
        if i > 20:
            break

def group_report(engine, dest_dir):
    groups = list(group_data(engine))
    num = len(groups)
    shows = filter(lambda g: g.get('num_sources', 0) > 0, groups)
    valids = filter(lambda g: g.get('num_entries', 0) > 0, groups)
    cover = filter(lambda g: g.get('num_entries', 0) > 0, groups)
    def within(groups, field, format_, **kw):
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
        return within(groups, 'last_modified', '%Y-%m-%dT%H:%M:%S', **kw)
    
    def within_c(groups, **kw):
        return within(groups, 'latest', '%Y-%m-%d', **kw)
    
    stats = {
        'num': len(groups),
        'numf': float(len(groups)),
        'reported_ever': len(shows),
        'reported_3m': len(within_m(shows, weeks=12)),
        'reported_6m': len(within_m(shows, weeks=26)),
        'reported_1y': len(within_m(shows, weeks=52)),
        'valid_ever': len(valids),
        'valid_3m': len(within_m(valids, weeks=12)),
        'valid_6m': len(within_m(valids, weeks=26)),
        'valid_1y': len(within_m(valids, weeks=52)),
        'cover_ever': len(valids),
        'cover_3m': len(within_c(valids, weeks=12)),
        'cover_6m': len(within_c(valids, weeks=26)),
        'cover_1y': len(within_c(valids, weeks=52)),
        }
    pprint(stats)
    report_ts = datetime.datetime.utcnow().strftime("%B %d, %Y")
    write_report(dest_dir, 'publishers.html',
            'index', groups=groups, stats=stats,
            report_ts=report_ts)

def resource_query(engine):
    data = {}
    q = """
        SELECT 
            src.*,
            COUNT(spe.id) AS num_entries,
            SUM(spe."AmountFormatted") AS total,
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
            """ SELECT message, stage FROM issue WHERE resource_id = '%s' AND resource_hash = '%s'
                ORDER BY timestamp DESC """ % (res['resource_id'], res['retrieve_hash']))))
        issues = set([(i['stage'], i['message']) for i in issues])
        res['issues'] = issues
        pn = res['publisher_name']
        if pn is None:
            continue
        if not pn in data:
            data[pn] = []
        data[pn].append(res)
    return data

def resource_report(engine, dest_dir):
    data = resource_query(engine)
    for publisher_name, resources in data.items():
        write_report(dest_dir, 'resources.html', 
            'publisher-' + publisher_name,
            resources=resources,
            publisher_name=publisher_name,
            publisher_title=resources[0].get('publisher_title'))

def create_report(dest_dir):
    if not os.path.isdir(dest_dir):
        os.makedirs(dest_dir)
    engine = db_connect()
    group_report(engine, dest_dir)
    resource_report(engine, dest_dir)

if __name__ == '__main__':
    import sys
    create_report(sys.argv[1])
