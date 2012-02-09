import os
import sqlaload as sl

def source_path(row):
    source_dir = 'sources'
    if not os.path.isdir(source_dir):
        os.makedirs(source_dir)
    return os.path.join(source_dir, row['resource_id'])

def db_connect():
    return sl.connect("postgresql:///uk25k")
