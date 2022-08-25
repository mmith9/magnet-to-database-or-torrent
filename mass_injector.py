#!/usr/bin/env python -u
"""
Merging sparse files is woefully inefficient when written in python.
Single byte comparison is in fact an object to object comparison

This is a crude attempt to optimize things without resorting to asm or C code
which would be a blitz

"""

from ast import Bytes
import os
import time
import pprint


VERSION = "0.0.1"


def merge_chunks(chunk1: Bytes, chunk2: Bytes) -> bytearray:
    actual_size = len(chunk1)
    if chunk1 == bytes(actual_size):  # is all zeros
        merged_chunk = chunk2
    elif chunk2 == bytes(actual_size):  # is all zeros
        merged_chunk = chunk1
    else:
        actual_size = len(chunk1)
        merged_chunk = bytearray(actual_size)
        for i in range(actual_size):
            if chunk1[i] >= chunk2[i]:
                merged_chunk[i] = chunk1[i]
            else:
                merged_chunk[i] = chunk2[i]

    return merged_chunk


def merge(filename1,filename2):
    chunk_size = args.chunk_size
    args.buffer_size = args.buffer_size

    chunks_num = 0
    f1_empty_chunks = 0
    f2_empty_chunks = 0
    same_non_zero_chunks = 0
    different_and_non_zero = 0
    merged = 0

    f1_buffer = []
    f2_buffer = []

    empty_chunk = bytes(chunk_size)
    f1_size = os.stat(filename1).st_size
    f2_size = os.stat(filename2).st_size
    assert f1_size == f2_size

    print('merging:')
    print(filename2)
    print('into')
    print(filename1)

    fh1 = open(filename1, 'r+b')
    fh2 = open(filename2, 'rb')

    chunk1 = True
    chunk2 = True
    while chunk1:
        if args.buffer_size > 0:
            if len(f1_buffer) == 0:
                while chunk1 and len(f1_buffer) < args.buffer_size:
                    chunk1 = fh1.read(chunk_size)
                    f1_buffer.append(chunk1)
                while chunk2 and len(f2_buffer) < args.buffer_size:
                    chunk2 = fh2.read(chunk_size)
                    f2_buffer.append(chunk2)
            try:
                chunk1 = f1_buffer.pop(0)
                chunk2 = f2_buffer.pop(0)
            except IndexError:
                fh1.close()
                fh2.close()
                return

        else:
            chunk1 = fh1.read(chunk_size)
            chunk2 = fh2.read(chunk_size)

        chunks_num += 1

        if args.stats:
            if chunk1 == empty_chunk:
                f1_empty_chunks += 1
            elif chunk1 == chunk2:
                same_non_zero_chunks += 1
            elif chunk2 != empty_chunk:
                different_and_non_zero += 1

            if chunk2 == empty_chunk:
                f2_empty_chunks += 1

        if chunk1 != chunk2:

            #            new_chunk = merge_chunks(chunk1, chunk2)

            actual_size = len(chunk1)
            if chunk1 == bytes(actual_size):  # is all zeros
                fh1.seek((chunks_num - 1) * chunk_size)
                fh1.write(chunk2)
                merged += 1


        if (chunks_num % 100) == 0:
            if args.stats:
                print('\rProgress chunks: {}={}%, merged {}={}% , f1 empty {},\
                    f2 empty {}, same non 0 {}, diff and non 0 {}'
                      .format(chunks_num, int(chunks_num*chunk_size/f1_size*100),
                              merged, int(merged*chunk_size/f1_size*10000)/100,
                              f1_empty_chunks, f2_empty_chunks, same_non_zero_chunks, different_and_non_zero),
                      end='')
            else:
                print('\rProgress chunks: {}={}%, merged {}={}%'.format(
                    chunks_num, int(chunks_num*chunk_size/f1_size*100),
                    merged, int(merged*chunk_size/f1_size*10000)/100),
                    end='')

    print()
    print('total chunks', chunks_num)
    print('merged', merged)
    print('f1 empty', f1_empty_chunks)
    print('f2 empty', f2_empty_chunks)
    print('same non zeros', same_non_zero_chunks)
    print('diff and non zero', different_and_non_zero)
    fh1.close()
    fh2.close()
    return


class FilesWithSizes:
    def __init__(self) -> None:
        self.files = {}
        self.count_items = 0

    def remove_size(self, size):
        if self.got_size(size):
            self.count_items -= len(self.files[size])
            del self.files[size]

    def get_all_sizes(self):
        return self.files.keys()

    def add_file(self, file, size):
        if self.got_size(size):
            self.files[size].append(file)
        else:
            self.files[size] = [file]
        self.count_items += 1

    def got_size(self, size) -> bool:
        if size in self.get_all_sizes():
            return True
        else:
            return False

    def get_files(self, size):
        if size in self.get_all_sizes():
            return self.files[size]
        else:
            return []


class MassInjector:
    def __init__(self, temps_dir: str, donors_dirs: list) -> None:
        self.temps_dir = temps_dir
        self.donors_dirs = donors_dirs
        self.temp_files = FilesWithSizes()
        self.donor_files = FilesWithSizes()
        self.file_pairs = []

    def scan_dirs(self):
        self.scan_dir(self.temps_dir, is_temp_dir=True)
        for path in self.donors_dirs:
            self.scan_dir(path, is_temp_dir=False)

    def scan_dir(self, root_path, is_temp_dir=False):
        count = 0
        print('Scanning', root_path)
        for path, _, filenames in os.walk(root_path):
            for filename in filenames:
                count += 1
                print(count, end='\r')
                filepath = os.path.join(path, filename)
                try:
                    filesize = os.lstat(filepath).st_size
                except FileNotFoundError:
                    logger.warning(
                        'File not found - can\'t get size of %s', filepath)

                if filesize > args.min_size:
                    if is_temp_dir:
                        if filename.lower().endswith('.!qb'):
                            self.temp_files.add_file(filepath, filesize)
                        if not args.skip_both_partials:
                            self.donor_files.add_file(filepath, filesize)
                    else:
                        if self.temp_files.got_size(filesize):
                            self.donor_files.add_file(filepath, filesize)
        print()

        # only after all .!qb files are accounted for can trim donors from temp dir
        if is_temp_dir:
            # problem with iterating over dict keys
            # solved with iterate - copy (deep copy won't pickle)
            all_sizes = []
            for x in self.donor_files.get_all_sizes():
                all_sizes.append(x)

            for size in all_sizes:
                if not self.temp_files.got_size(size):
                    self.donor_files.remove_size(size)

    def find_pairs(self):
        for size in self.temp_files.get_all_sizes():
            for temp_file in self.temp_files.get_files(size):
                for donor_file in self.donor_files.get_files(size):
                    if temp_file != donor_file:
                        if args.skip_both_partials and \
                            donor_file.lower().endswith('.!qb'):
                            logger.debug('autoskip partial donor')
                        else:
                            self.file_pairs.append(
                                {'donor': donor_file, 'recipient': temp_file, 'similarity': (0, 0, 0), 'size': size})
                    else:
                        logger.debug('donor is same as recipient')

    def merge_files(self, recipient, donor):
        logger.info(' injecting \n%s \ninto \n%s', donor, recipient)

    def compute_similarities(self, coverage):
        count = 0
        for pair in self.file_pairs:
            try:
                fh1 = open(pair['donor'], 'rb')
            except FileNotFoundError:
                logger.warning('File not found\n%s', pair['donor'])
                return
            try:
                fh2 = open(pair['recipient'], 'rb')
            except FileNotFoundError:
                logger.warning('File not found\n%s', pair['recipient'])
                return
            pair['similarity'] = self.compute_similarity(
                fh1, fh2, pair['size'], coverage)
            if pair['similarity'][1] < 50:
                print()
                pair['similarity'] = self.compute_similarity(
                fh1, fh2, pair['size'], 0.05, autoadjust=False)
            if pair['similarity'][1] < 50:
                print()
                pair['similarity'] = self.compute_similarity(
                fh1, fh2, pair['size'], 0.2, autoadjust=False)

            fh1.close()
            fh2.close()
            logger.debug('pair similarity %s\n%s\n%s',
                        pair['similarity'], pair['recipient'], pair['donor'])
            count +=1
            print('pair {} of {}'. format(count, len(self.file_pairs)), end='          \r')

    def compute_similarity_pair(self, pair, coverage):
        try:
            fh1 = open(pair['donor'], 'rb')
        except FileNotFoundError:
            logger.warning('File not found\n%s', pair['donor'])
            return
        try:
            fh2 = open(pair['recipient'], 'rb')
        except FileNotFoundError:
            logger.warning('File not found\n%s', pair['recipient'])
            return
        pair['similarity'] = self.compute_similarity(
            fh1, fh2, pair['size'], coverage, autoadjust=False)
        fh1.close()
        fh2.close()
        logger.info('pair similarity %s\n%s\n%s',
                    pair['similarity'], pair['recipient'], pair['donor'])

    def compute_similarity(self, fh1, fh2, size, coverage, autoadjust=True):
        same_blocks = 0
        non_zero_blocks = 0

        total_blocks = int(coverage * size / args.chunk_size)
        while autoadjust and total_blocks > 150:
            coverage = coverage /1.3
            total_blocks = int(coverage * size / args.chunk_size)

        while autoadjust and total_blocks < 100:
            coverage = coverage *1.3
            total_blocks = int(coverage * size / args.chunk_size)

        for x in range(total_blocks):
            fh1.seek(int(x * args.chunk_size * (1/coverage)))
            chunk1 = fh1.read(args.chunk_size)
            fh2.seek(int(x * args.chunk_size * (1/coverage)))
            chunk2 = fh2.read(args.chunk_size)

            actual_size = len(chunk1)
            if (chunk1 != bytes(actual_size)) and (chunk2 != bytes(actual_size)):
                non_zero_blocks += 1
                if chunk1 == chunk2:
                    same_blocks += 1
            if not autoadjust:
                print(x, 'of', total_blocks, end='                   \r')

        return (same_blocks, non_zero_blocks, total_blocks)


def main():
    temps = args.dirs[0]
    if len(args.dirs) > 1:
        donors = args.dirs[1:]
    else:
        donors = []
    injector = MassInjector(temps, donors)

    injector.scan_dirs()

    # pp=pprint.PrettyPrinter()

    # pp.pprint(injector.temp_files.files)
    # print()
    # pp.pprint(injector.donor_files.files)

    #print(injector.temp_files.count_items)
    #print(injector.donor_files.count_items)

    injector.find_pairs()
    injector.compute_similarities(0.01)
    print()
    for pair in injector.file_pairs:
        choice = ''

        while choice not in ['s', 'd', 'm']:
            print()
            print()
            print('size {}MiB pair similarity {}, recipient\n{}'.format(
                int(pair['size']*100/1024/1024)/100, pair['similarity'], pair['recipient']))
            print()
            print('donor\n{}'.format(pair['donor']))
            print('(m)erge, (d)elete recipient, (r)ecalculate similarity, (s)kip')

            if args.skip_zero_sim and pair['similarity'][0] == 0:
                choice = 's' #autoskip
                print('0 similiarities, autoskip')
            else:
                choice = input()
                if choice == 'r':
                    choice = input('% of file to check> ')
                    injector.compute_similarity_pair(pair, int(choice)/100)

        if choice == 's':
            pass
        elif choice == 'd':
            try:
                os.remove(pair['recipient'])
            except FileNotFoundError:
                logger.error('File not found %s', pair['recipient'])
        elif choice == 'm':
            merge(pair['recipient'], pair['donor'])


if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(__name__ + '.txt')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(formatter)

    # logger.addHandler(fh)
    logger.addHandler(sh)

    from argparse import ArgumentParser
    parser = ArgumentParser(
        description='Merge into all .!qb in partial torrent dir, \
                    from all dirs, torrent temp included')

    parser.add_argument('dirs', help='first dir - torrents temp, res dirs - donors',
                        type=str, nargs='+')

    parser.add_argument('-s', '--stats', dest='stats', default=False, action='store_true',
                        help='compute some more detailed stats')

    parser.add_argument('-b', '--buffersize', dest='buffer_size', default=100, type=int,
                        help='buffer MiB size (two files, two buffers) for same disc read speed up')

    parser.add_argument('-c', '--chunksize', dest='chunk_size', default=16, type=int,
                        help='internal chunk to deal with size in KiB')

    parser.add_argument('--nopartials', dest='skip_both_partials', default=False,
                        action='store_true', help='autoskip if donor is partial')

    parser.add_argument('--nozerosim', dest='skip_zero_sim', default=True,
                        action='store_false', help='autoskip if 0 similarity')


    parser.add_argument('-m', '--minsize', dest='min_size', default=50, type=int,
                        help='minimum file size to consider MiB')

    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()

    args.buffer_size = args.buffer_size * 1024 / args.chunk_size
    args.chunk_size *= 1024

    args.min_size = args.min_size * 1024 * 1024  # MiB

    start = time.time()
    main()
    end = time.time()
    total_time = end - start
    print("\nExecution time: " + str(total_time))
