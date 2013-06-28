'''
Chops a CSV file up into smaller files, to be more manageable.

This task is slightly complicated due to the presence of carriage returns
within strings in lines, which you need to keep together. And you need the
header repeated in each file.

Works line-by-line, so does not use much memory.
'''

import argparse
import sys
import os.path
import csv
import _csv

class ChopError(Exception):
    pass

def csvchop(csv_filepath, max_part_size, overwrite=False):
    # Check CSV exists
    csv_filepath = os.path.expanduser(csv_filepath)
    if not os.path.exists(csv_filepath):
        raise ChopError('Could not find CSV file: %s' % previous_csv_filepath)

    # Open the CSV
    with open(csv_filepath, 'rb') as in_file:
        # header
        header = in_file.readline()
        part_file = PartFiles(csv_filepath, header, overwrite=overwrite)

        try:
            for line, row in csv_rows(in_file):
                part_file.write(line)
                if part_file.get_length() >= max_part_size:
                    part_file.close_current_file()
        finally:
            part_file.close_current_file()

class PartFiles:
    def __init__(self, filepath_base, header, overwrite=False):
        self.filepath_base = filepath_base
        self.header = header
        self.overwrite = overwrite

        self.current_file = None
        self.part_index = 1
        self.current_file_length = 0

    def write(self, line):
        if not self.current_file:
            # open a new part file
            self.filepath = '%s.%i' % (self.filepath_base, self.part_index)
            if not self.overwrite and os.path.exists(self.filepath):
                raise ChopError('File in the way: %s Try the --overwrite option' % self.filepath)
            self.current_file = open(self.filepath, 'wb')
            self.current_file.write(self.header)
            self.current_file_length = len(self.header)
            self.part_index += 1
        line_to_write = line + '\n'
        self.current_file.write(line_to_write)
        self.current_file_length += len(line_to_write)

    def get_length(self):
        '''return the length of the current file'''
        return self.current_file_length

    def close_current_file(self):
        if self.current_file:
            self.current_file.close()
            self.current_file = None
            print 'Written %s (%s bytes)' % (self.filepath, self.current_file_length)

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Chops a CSV file up into smaller files.')
    parser.add_argument('csv_filepath', metavar='FILE.CSV', type=str,
                        help='Filepath of the CSV file')
    parser.add_argument('part_size', metavar='SIZE-BYTES', type=int,
                        help='Max size of the parts to chop the CSV into (bytes).')
    parser.add_argument('--overwrite', dest='overwrite', action='store_true', default=False,
                        help='Whether to overwrite existing files')

    args = parser.parse_args()
    try:
        csvchop(args.csv_filepath, args.part_size, args.overwrite)
    except ChopError, e:
        print >> sys.stderr, 'ERROR: %s\n' % e
        parser.print_help(argparse._sys.stderr)
        sys.exit(1)
