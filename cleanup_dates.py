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
    '%d/%m/%Y', '%d/%m/%Y %H:%M', '%m/%d/%Y', '%d/%m/%y', '%d.%m.%y', '%d.%m.%Y',
    # Ridiculous ones
    '%m/%d/%y', '%Y%m%d',
    # Things with words in
    '%d-%b-%y', '%d-%b-%Y', '%d/%b/%Y', '%b-%y', '%d %B %Y', '%d %b %y',
    # Things we'd prefer to forget
    'excel']

def detect_format(values):
    # TODO: alternative solution - some sheets use more than one date format, 
    # could pass a priorized list and attempt each?
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
    scores = sorted(scores.items(), key=lambda (f,n): n)
    #print scores
    if not len(scores):
        return Non
    return scores[-1][0]

def detect_formats(data):
    formats = {}
    for field in DATE_FIELDS:
        values = [r.get(field, '').strip() for r in data]
        formats[field] = detect_format(values)
    return formats

def apply(row, formats):
    for field, format_ in formats.items():
        try:
            value = row.get(field)
            if value is None:
                continue
            if format_ == 'excel':
                # Deciphers excel dates that have been mangled into integers by
                # formatting errors
                parsed = datetime(*xldate_as_tuple(float(field.strip()), 0))
            else:
                parsed = datetime.strptime(value.strip(), format_)
            row[field + 'Formatted'] = parsed.strftime("%Y-%m-%d")
        except Exception as e:
            row[field + 'Formatted'] = None
            row['valid'] = False
            #log.exception(e)
    return row

