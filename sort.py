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
    """Функция вычитывает указанную порцию строк в память, если необходимо подрезает строку по максимальной длине,
    далее пробует их разделить, и взять заданную колонку, по которой будет отсортирована эта порция. Если колонки нету,
    то пишет в общий файл неотсортированных файлов.
    """

    logging.info('Разделение данных на отдельные сортированные файлы')
    with open(file_name, 'r') as source_file, open(UNSORTED_FILE_PREFIX+file_name, 'w') as unsorted_file:

        i = 1
        temp_file = open(TEMP_FILE_NAME.format(i), 'w', buffering=1)
        lines = []
        for line in source_file:
            if len(line) > max_line_length:
                line = line[:max_line_length]
                logging.warning('Достигнута макс длина. Строка обрезана')

            spl_line = line.split(column_separator)
            try:
                c = spl_line[column_number]
                lines.append((c, line))
            except IndexError:
                logging.error('Строка не имеет колонки. Записана в файл {}'.format(unsorted_file.name))
                unsorted_file.write(line)

            if len(lines) == tempfile_line_count:
                lines.sort(key=lambda el: el[0])
                temp_file.writelines([t[1] for t in lines])
                temp_file.close()
                lines.clear()
                logging.info('Файл {} заполнен'.format(temp_file.name))
                i += 1
                temp_file = open(TEMP_FILE_NAME.format(i), 'w')

        logging.info('Файл {} заполнен'.format(temp_file.name))
    logging.info('Разделение завершено')


def merge(file_name, column_separator, column_number):
    """Функция читает временные файлы, зная, что они уже отсортированы по возрастанию,
    чтобы применить heapq.merge() с записью в результирующий большой файл. Чтение и запись происходит построчно потоком.
    """

    logging.info('Слияние в конечный отсортированный файл')
    with open(SORTED_FILE_PREFIX+file_name, 'w') as merged_file:
        ifiles = []
        for path in (p for p in os.listdir() if p.startswith(TEMP_FILE_PREFIX)):
            ifile = open(path, 'r')
            ifiles.append(ifile)

        for line in heapq.merge(*ifiles, key=lambda line: line.split(column_separator)[column_number]):
            merged_file.write(line)
    logging.info('Слияние завершено')


def fill(file_name, samplefile_line_count, column_number):
    """Вспомогательная функция. Пишет случайные текстовые данные с разным количеством колонок.
    """

    logging.info('Заполнение тестовых данных размером')
    with open(file_name, 'w') as file:
        population = string.ascii_letters + string.digits
        count = 0
        while count < samplefile_line_count:
            line = ','.join(''.join(sample(population, 10)) for _ in range(randint(1, column_number+2))) + '\n'
            file.write(line)
            count += 1
    logging.info('Заполнение завершено')


class ProfileIt:

    def __enter__(self):
        self.p = cProfile.Profile()
        self.p.enable()

    def __exit__(self, exc_type, exc_val, exc_tb):
         self.p.disable()
         stream = io.StringIO()
         stats = pstats.Stats(self.p, stream=stream).sort_stats('cumulative')
         stats.print_stats()
         logging.debug('Измерение вызовов\n' + stream.getvalue())


def main(ns):
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Старт')

    def do_logic():
        if not os.path.exists(ns.file):
            fill(ns.file, ns.samplefile_line_count, ns.column_number)
        split_sort(ns.file, ns.max_line_length, ns.tempfile_line_count, ns.column_separator, ns.column_number)
        merge(ns.file, ns.column_separator, ns.column_number)

    try:
        if ns.profile:
            with ProfileIt():
                do_logic()
        else:
            do_logic()
    except UnicodeDecodeError:
        logging.exception('Не могу прочитать файл')
    except PermissionError:
        logging.exception('Недостаточно прав к файлу')
    except:
        logging.exception('Сложная ситуация...')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('file')
    parser.add_argument('--column-separator', default=',', help='Очевидно разделитель строки по колонкам')
    parser.add_argument('--column-number', type=int, default=0,
                        help='Колонка, по которой будет производиться сортировка,'
                             'а также значение+2 случайного количества колонок при заполнении тестовых данных')
    parser.add_argument('--max-line-length', type=int, default=1000,
                        help='Максимально допустимая длина строки, до которой будет обрезана превышающая')
    parser.add_argument('--tempfile-line-count', type=int, default=100000,
                        help='Порция строк, сортируемая и сохраняемая в раздельный файл. '
                             'Может быть меньше в файле в результате отсутвия искомой колонки')
    parser.add_argument('--samplefile-line-count', type=int, default=3000000, help='Количество строк в файле тестовых данных')
    parser.add_argument('--profile', action='store_true', help='Выводить измерение вызовов')
    ns = parser.parse_args()
    main(ns)
