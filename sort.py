import os
import string
import heapq
import logging
import argparse
import cProfile, pstats, io
from random import sample, randint


TEMP_FILE_PREFIX = 'temp'
TEMP_FILE_NAME = TEMP_FILE_PREFIX + '{}.txt'
UNSORTED_FILE_PREFIX = 'unsorted_'
SORTED_FILE_PREFIX = 'sorted_'


def split_sort(file_name, max_line_length, tempfile_line_count, column_separator, column_number):
    """Function reads specfied portion of lines into memory, trims line if needed to max length, tries to split them,
    gets column number of sorting. If there is no such column it writes to unsorted file.
    """

    logging.info('Data splitting to sorted files')
    with open(file_name, 'r') as source_file, open(UNSORTED_FILE_PREFIX+file_name, 'w') as unsorted_file:

        i = 1
        temp_file = open(TEMP_FILE_NAME.format(i), 'w', buffering=1)
        lines = []
        for line in source_file:
            if len(line) > max_line_length:
                line = line[:max_line_length]
                logging.warning('Max length is reached. Line is trimmed')

            spl_line = line.split(column_separator)
            try:
                c = spl_line[column_number]
                lines.append((c, line))
            except IndexError:
                logging.error('Line has not column. Write to the file {}'.format(unsorted_file.name))
                unsorted_file.write(line)

            if len(lines) == tempfile_line_count:
                lines.sort(key=lambda el: el[0])
                temp_file.writelines([t[1] for t in lines])
                temp_file.close()
                lines.clear()
                logging.info('File {} is filled'.format(temp_file.name))
                i += 1
                temp_file = open(TEMP_FILE_NAME.format(i), 'w')

        logging.info('File {} is filled'.format(temp_file.name))
    logging.info('Splitting is completed')


def merge(file_name, column_separator, column_number):
    """Function reads temporary files knowing they are sorted by ascending to do heapq.merge()
    to write in the resulting file. Reading and writing are executed by line.
    """

    logging.info('Merge to a sorted resulting file')
    with open(SORTED_FILE_PREFIX+file_name, 'w') as merged_file:
        ifiles = []
        for path in (p for p in os.listdir() if p.startswith(TEMP_FILE_PREFIX)):
            ifile = open(path, 'r')
            ifiles.append(ifile)

        for line in heapq.merge(*ifiles, key=lambda line: line.split(column_separator)[column_number]):
            merged_file.write(line)
    logging.info('Merge is completed')


def fill(file_name, samplefile_line_count, column_number):
    """Auxiliary function. It writes random text data with a diffenrent count of columns in a line.
    """

    logging.info('Data filling')
    with open(file_name, 'w') as file:
        population = string.ascii_letters + string.digits
        count = 0
        while count < samplefile_line_count:
            line = ','.join(''.join(sample(population, 10)) for _ in range(randint(1, column_number+2))) + '\n'
            file.write(line)
            count += 1
    logging.info('Filling is completed')


class ProfileIt:

    def __init__(self, flag=True):
        self.flag = flag
        self.p = None

    def __enter__(self):
        if self.flag:
            self.p = cProfile.Profile()
            self.p.enable()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.flag:
            self.p.disable()
            stream = io.StringIO()
            stats = pstats.Stats(self.p, stream=stream).sort_stats('cumulative')
            stats.print_stats()
            logging.debug('Callings measure\n' + stream.getvalue())


def main(ns):
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Start')

    def do_logic():
        if not os.path.exists(ns.file):
            fill(ns.file, ns.samplefile_line_count, ns.column_number)
        split_sort(ns.file, ns.max_line_length, ns.tempfile_line_count, ns.column_separator, ns.column_number)
        merge(ns.file, ns.column_separator, ns.column_number)

    try:
        with ProfileIt(ns.profile):
            do_logic()
    except UnicodeDecodeError:
        logging.exception('Can not read the file)
    except PermissionError:
        logging.exception('There are not Permissions')
    except:
        logging.exception('Unpredictable event...')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('file')
    parser.add_argument('--column-separator', default=',')
    parser.add_argument('--column-number', type=int, default=0,
                        help='Column will be used to sort,'
                        'and parameter value+2 of random columns count during the data filling')
    parser.add_argument('--max-line-length', type=int, default=1000,
                        help='Max length of string will be used to trim if exceeds')
    parser.add_argument('--tempfile-line-count', type=int, default=100000,
                        help='Line portion are sorted and saved in an another file.
                             'It is possible it will be less if the column number is not found')
    parser.add_argument('--samplefile-line-count', type=int, default=3000000, help='Lines count is in the data file')
    parser.add_argument('--profile', action='store_true', help='Display a profiling measurement')
    ns = parser.parse_args()
    main(ns)
