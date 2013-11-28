import logging
from collections import defaultdict
from datetime import datetime
from xlrd.xldate import xldate_as_tuple

log = logging.getLogger('cleanup')
DATE_FIELDS = ['Date']

FORMATS = [
    # Variations on sensible date formats
    '%Y-%m-%d', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
    # Less sensible date formats
    '%d/%m/%Y', '%d/%m/%Y %H:%M', '%m/%d/%Y %H:%M', '%m/%d/%Y', '%d/%m/%y', '%d.%m.%y', '%d.%m.%Y', '%d-%m-%Y',
    # Ridiculous ones
    '%m/%d/%y', '%Y%m%d',
    # Things with words in
    '%d-%b-%y', '%d-%b-%Y', '%d/%b/%Y', '%b-%y', '%d %B %Y', '%d %b %y',
    # Things we'd prefer to forget
    'excel']

def detect_format(values):
    '''Given a list of dates, return the date format strings that matches them best,
    with the best matching one first.'''
    if not len(values):
        return None
    values_ = defaultdict(int)
    for value in values:
        values_[value] += 1

    scores = defaultdict(int)
    for (value, weight) in values_.items():
        if value is None:
            continue
        for format_ in FORMATS:
            try:
                if format_ == 'excel':
                    # Since it's the only integer format supported, this isn't bad. It's 1982 .. 2036 ish
                    assert float(value) > 30000 and float(value) < 50000
                else:
                    datetime.strptime(value.strip(), format_)
                scores[format_] += weight
            except: pass
    # Sort highest weighted/scoring formats first
    scores = sorted(scores.items(), key=lambda (format_, weight): -weight)
    if not len(scores):
        log.debug("Date Values: %r", set(values))
        return 'Could not understand the date format e.g. "%s"' % value
    # Filter out formats with less than 25% of the highest score - assume these
    # are false positives
    max_weight = scores[0][1]
    weight_threshold = int(float(max_weight) * 0.25)
    if weight_threshold < 1:
        weight_threshold = 1
    formats = [score[0] for score in scores if score[1] >= weight_threshold]
    #print scores
    return formats

def detect_formats(data):
    '''Given data (list of rows), it returns a dict of each Date field/column
    with a list of formats.  If there is an error detecting for a column, the value in
    the dict will be the error string.
    '''
    if not data:
        log.warning('Table has no rows')
        return dict(zip(DATE_FIELDS, ['Table has no rows']*len(DATE_FIELDS)))
    field_formats = {}
    for field in DATE_FIELDS:
        values = [r.get(field) for r in data]
        values = [v.strip() for v in values if v]
        if not values:
            log.warning('Date column "%s" has no values', field)
            field_formats[field] = 'Date column has no values'
            continue
        field_formats[field] = detect_format(values)
    return field_formats

def apply(row, field_formats, stats):
    today = datetime.now()
    for field, formats in field_formats.items():
        try:
            value = row.get(field)
            if value in (None, ''):
                stats[field].add_spending('Empty', row)
                continue
            parsed = None
            # Try parsing
            for format_ in formats:
                try:
                    if format_ == 'excel':
                        # Deciphers excel dates that have been mangled into integers by
                        # formatting errors
                        parsed = datetime(*xldate_as_tuple(float(field.strip()), 0))
                    else:
                        parsed = datetime.strptime(value.strip(), format_)
                    break
                except Exception, e:
                    pass
            if not parsed:
                row[field + 'Formatted'] = None
                row['valid'] = False
                stats[field].add_spending('Parse error', row, value)
                continue
            # Check it is not in the future - an obvious mistake
            if parsed > today:
                row[field + 'Formatted'] = None
                row['valid'] = False
                stats[field].add_spending('Date in the future', row, parsed)
                continue
            formatted_date = parsed.strftime("%Y-%m-%d")
            stats[field].add_spending('Parsed ok', row, value)
            row[field + 'Formatted'] = formatted_date
        except Exception as e:
            row[field + 'Formatted'] = None
            row['valid'] = False
            stats[field].add_spending('Exception %s' % e.__class__.__name__, row)
            log.exception(e)
    return row

