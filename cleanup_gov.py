import sqlaload as sl
import nkclient as nk

from common import *

CACHE = {}

GOV_FIELDS = [
    ('EntityName', 'uk25k-entities'),
    ('DepartmentFamilyName', 'uk25k-departments')
    ]

def apply(row):
    for field, dataset in GOV_FIELDS:
        out = field + 'Canonical'
        val = row.get(field)
        if (dataset, val) in CACHE:
            row[out] = CACHE[(dataset, val)]
            continue
        try:
            if val is None or not len(val):
                row[out] = None
            ds = nk_connect(dataset)
            v = ds.lookup(val)
            row[out] = v.value
        except nk.NKInvalid:
            row[out] = None
        except nk.NKNoMatch:
            row[out] = val
        CACHE[(dataset, val)] = row[out]
    return row



