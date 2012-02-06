import os

def source_path(row):
    source_dir = 'sources'
    if not os.path.isdir(source_dir):
        os.makedirs(source_dir)
    return os.path.join(source_dir, row['resource_id'])
