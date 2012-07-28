
NUMERIC_FIELDS = ['Amount', 'VATNumber']

def apply(row):
    for field in NUMERIC_FIELDS:
        try:
            val = row.get(field)
            if val is None:
                raise ValueError()
            val = "".join([v for v in val if v in "-.0123456789"])
            row[field + 'Formatted'] = float(val)
        except Exception as e:
            row[field + 'Formatted'] = None
    return row


