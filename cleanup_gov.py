import sqlaload as sl

from common import *

CACHE = {}

GOV_FIELDS = [
    ('EntityName', 'uk25k-entities'),
    ('DepartmentFamilyName', 'uk25k-departments')
    ]

log = logging.getLogger('cleanup_gov')

def apply(row, stats_dict):
    for field, dataset in GOV_FIELDS:
        out = field + 'Canonical'
        val = row.get(field)
        stats = stats_dict['field']
        try:
            if (dataset, val) in CACHE:
                row[out] = CACHE[(dataset, val)]
                if row[out] == None:
                    stats.add_spending('Invalid/NoMatch', row)
                else:
                    stats.add_spending('Match', row)
                continue
            try:
                if val is None or not len(val):
                    row[out] = None
                ds = nk_connect(dataset)
                v = ds.lookup(val)
                row[out] = v.name
                stats.add_spending('Match', row)
            except ds.Invalid:
                row[out] = None
                stats.add_spending('Invalid', row)
            except ds.NoMatch:
                row[out] = None
                stats.add_spending('No Match', row)
            CACHE[(dataset, val)] = row[out]
        except Exception, e:
            stats.add_spending('Exception %s' % e.__class__.__name__, row)
            log.exception(e)
    return row



