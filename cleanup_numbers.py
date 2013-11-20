
NUMERIC_FIELDS = ['Amount', 'VATNumber']

def apply(row, stats):
    for field in NUMERIC_FIELDS:
        val = row.get(field)
        try:
            if val in (None, '', 'None') or not unicode(val).strip():
                stats[field].add_spending('Empty', row)
                row[field + 'Formatted'] = None
                continue
            val = "".join([v for v in val if v in "-.0123456789"])
            stats[field].add_spending('Parsed ok', row, val)
            row[field + 'Formatted'] = float(val)
        except Exception as e:
            stats[field].add_spending('Exception: %s' % e.__class__.__name__, row, val)
            row[field + 'Formatted'] = None
    return row

