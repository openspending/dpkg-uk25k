'''
Diffs spending data, so you can load into OpenSpending only the transactions
that are new or changed, compared to the last load.

The reason you cannot use (the other) 'diff' to do this is because of the
presence of carriage returns within lines. This program handles them correctly.

Rows that from the Previous CSV that are not present in New CSV are ignored,
as OpenSpending does not handle transaction removal from its database.

Each CSV containing transaction data can be gigabytes large, but this script
does not load it all into memory. It loads the IDs and 8 byte checksums for every
line in Previous CSV into memory and then works through the New CSV with a line
in memory at a time.
'''

import argparse
import sys
import os.path
import csv
import _csv
import hashlib
import re
import shlex

class DiffError(Exception): pass

str1 = ''
str2 = ''

def spend_diff(previous_csv_filepath, new_csv_filepath, key_column):
    # Check CSVs exist
    previous_csv_filepath = os.path.expanduser(previous_csv_filepath)
    if not os.path.exists(previous_csv_filepath):
        raise DiffError('Could not find Previous CSV file: %s', previous_csv_filepath)
    new_csv_filepath = os.path.expanduser(new_csv_filepath)
    if not os.path.exists(new_csv_filepath):
        raise DiffError('Could not find New CSV file: %s', new_csv_filepath)

    # Open the previous CSV and save the hashes of the lines
    previous_lines = {} # id: line hash
    with open(previous_csv_filepath, 'rb') as f:
        header = f.readline()
        header_cells = parse_csv_line(header)
        try:
            key_column_index = header_cells.index(key_column)
        except ValueError:
            raise DiffError('Could not find key %r in header column %r',
                            key_column, header_cells)
        for line, row in csv_rows(f):
            # Store hash
            key = row[key_column_index]
            previous_lines[key] = line_checksum(line)

    # Open the new CSV and print lines non-matching lines
    with open(new_csv_filepath, 'rb') as f:
        csv_reader = csv.reader(f, delimiter=',', quotechar='"')
        header = f.readline()
        header_cells = parse_csv_line(header)
        yield header.rstrip('\n\r')
        try:
            key_column_index = header_cells.index(key_column)
        except ValueError:
            raise DiffError('Could not find key %r in header column %r',
                            key_column, header_cells)
        for line, row in csv_rows(f):
            # Transaction row, so compare hash
            key = row[key_column_index]
            previous_hash = previous_lines.get(key)
            if previous_hash:
                if previous_hash == line_checksum(line):
                    # line is the same
                    continue
                else:
                    # line has changed
                    pass
            yield line.rstrip('\n\r')


def csv_rows(file_handler):
    '''Returns each row of a CSV as both a string and a list,
    working as a generator.

    Where there is a newline inside a string, the row returned
    will also contain the newline char.

    No trailing '\n' characters are returned
    '''
    row = ''
    for line in file_handler:
        row += line
        try:
            row_cells = parse_csv_line(row)
        except _csv.Error, e:
            if 'newline inside string' in str(e):
                continue
        yield row.rstrip('\n\r'), row_cells
        row = ''

def parse_csv_line(line):
    return list(csv.reader([line]))[0]

def line_checksum(line):
    return hashlib.md5(line).hexdigest()[:8]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Diffs spending data to find what is new and changed.')
    parser.add_argument('previous_csv', metavar='PREVIOUS.CSV', type=str,
                        help='Previous spend data')
    parser.add_argument('new_csv', metavar='NEW.CSV', type=str,
                        help='New spend data')
    parser.add_argument('key_column', metavar='KEY_COLUMN', type=str,
                        help='Title of the column which is the unique key')

    args = parser.parse_args()
    try:
        for line in spend_diff(args.previous_csv, args.new_csv, args.key_column):
            print line
    except DiffError, e:
        print 'ERROR', e
        parser.print_help()
        sys.exit(1)
