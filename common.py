import os
import sqlaload as sl

def source_path(row):
    source_dir = 'sources'
    if not os.path.isdir(source_dir):
        os.makedirs(source_dir)
    return os.path.join(source_dir, row['resource_id'])

def db_connect():
    return sl.connect("postgresql:///uk25k")

def normalise_header(h):
    h = h.lower().strip()
    h = h.replace('no.', 'number')
    h = h.replace(' ', '').replace('.', '').replace(',', '')
    return h

def normalise_header_map(headers):
    # This is pretty grotesque. We need to disambiguate between
    # multiple identically-named columns after
    # normalisation. Impedence mismatch with messytables here, it was
    # supposed to take care of this

    # Pass 1: normalise while counting the occurances of each normalised value
    norm_headers_count = {}
    norm_headers_tmp = []
    for h in headers:
        n = normalise_header(h)
        norm_headers_count.setdefault(n, 0)
        norm_headers_count[n] = norm_headers_count[n] + 1
        norm_headers_tmp.append((n,h))

    # We're going to need an incrementing counter for each header that occurs more than once
    norm_headers_count = {k:0 for k,v in norm_headers_count.iteritems() if v > 1}

    # Pass 2: assemble normalised headers from counters
    norm_headers = {}
    for n,h in norm_headers_tmp:
        if norm_headers_count.has_key(n):
            norm_headers_count[n] = norm_headers_count[n] + 1
            norm_headers[h] = "%s.%d" % (n, norm_headers_count[n])
        else:
            norm_headers[h] = n

    return norm_headers

def normalise_header_list(headers):
    return sorted(normalise_header_map(headers).values())

def normalise_columns_map(table):
    columns = filter(lambda c: c != 'id', map(lambda c: c.name, table.c))
    return normalise_header_map(columns)

def normalise_columns_list(table):
    return sorted(normalise_columns_map(table).values())
