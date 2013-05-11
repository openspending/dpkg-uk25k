import sqlaload as sl

from common import *

CACHE = {}

GOV_FIELDS = [
    ('EntityName', 'uk25k-entities'),
    ('DepartmentFamilyName', 'uk25k-departments')
    ]

log = logging.getLogger('cleanup_gov')

def apply(row):
    for field, dataset in GOV_FIELDS:
        out = field + 'Canonical'
        val = row.get(field)
        try:
            if (dataset, val) in CACHE:
                row[out] = CACHE[(dataset, val)]
                continue
            try:
                if val is None or not len(val):
                    row[out] = None
                ds = nk_connect(dataset)
                v = ds.lookup(val)
                row[out] = v.name
            except ds.Invalid:
                row[out] = None
            except ds.NoMatch:
                row[out] = None
            CACHE[(dataset, val)] = row[out]
        except Exception, e:
            log.exception(e)
    return row



