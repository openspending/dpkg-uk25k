
NUMERIC_FIELDS = ['Amount', 'VATNumber']

def apply(row, stats):
    for field in NUMERIC_FIELDS:
        try:
            val = row.get(field)
            if val is None:
                stats.add_spending('Empty', row)
                row[field + 'Formatted'] = None
                continue
            val = "".join([v for v in val if v in "-.0123456789"])
            stats.add_spending('Parsed ok', row)
            row[field + 'Formatted'] = float(val)
        except Exception as e:
            stats.add_spending('Exception: %s' % e.__class__.__name__, row)
            row[field + 'Formatted'] = None
    return row


