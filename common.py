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

def normalise_header_list(headers):
    norm_headers = map(normalise_header, headers)
    return sorted(norm_headers)

def normalise_header_map(headers):
    norm_headers = map(lambda h: (h, normalise_header(h)), headers)
    return dict(norm_headers, key=lambda x: x[0])
