#!/usr/bin/env python -u
"""
Merging sparse files is woefully inefficient when written in python.
Single byte comparison is in fact an object to object comparison

This is a crude attempt to optimize things without resorting to asm or C code
which would be a blitz

"""

from ast import Bytes

import hashlib
import os
import time
import io
import sys
import pprint
import sqlite3
import bencode


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


def merge(filename1, filename2):
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

    def reset(self):
        self.files = {}
        self.count_items = 0

    def remove_size(self, size):
        if self.got_size(size):
            self.count_items -= len(self.files[size])
            del self.files[size]

    def get_all_sizes(self):
        return self.files.keys()

    def add_file(self, filename, size):
        if self.got_size(size):
            self.files[size].append(filename)
        else:
            self.files[size] = [filename]
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

    def get_all_files(self):
        filelist = []
        for size, files in self.files.items():
            for filename in files:
                filelist.append({'size': size, 'name': filename})
        return filelist


class DbEntryWithSizes(FilesWithSizes):
    def add_entry(self, filename, size, db_index, db_tr_index):
        entry = {}
        entry['filename'] = filename
        entry['db_id'] = db_index
        entry['db_tr_index'] = db_tr_index
        return super().add_file(entry, size)


class MassTorr:
    def __init__(self, cursor, donors_dirs: list) -> None:
        self.donors_dirs = donors_dirs
        self.db_all_files = DbEntryWithSizes()
        self.donor_files = FilesWithSizes()
        self.file_pairs = []

        self.cursor = cursor
        self.db_id: int
        self.db_torrent_id: str
        self.db_name: str
        self.db_category: str
        self.db_layout: str
        self.db_metadata: bytes
        self.db_metainfo: bytes
        self.db_info = None
        self.db_pieces = None
        self.db_block_size: int

    def scan_dirs(self):
        #        if self.temps_dir:
        #            self.scan_dir(self.temps_dir, is_db_dir=True)
        for path in self.donors_dirs:
            self.scan_dir(path, is_db_dir=False)

    def scan_dir(self, root_path, is_db_dir=False):
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
                    self.donor_files.add_file(filepath, filesize)

                    # if is_db_dir:
                    #     if filename.lower().endswith('.!qb'):
                    #         self.db_all_files.add_file(filepath, filesize)
                    #     if not args.skip_both_partials:
                    #         self.donor_files.add_file(filepath, filesize)
                    # else:
                    #     if self.db_all_files.got_size(filesize):
                    #         self.donor_files.add_file(filepath, filesize)
        print()

    def find_pairs(self):
        self.file_pairs = []
        for size in self.db_all_files.get_all_sizes():
            for db_file in self.db_all_files.get_files(size):
                for donor_file in self.donor_files.get_files(size):
                    if db_file != donor_file:  # TODO it's actually much complicated now
                        if args.skip_both_partials and \
                                donor_file.lower().endswith('.!qb'):
                            logger.debug('autoskip partial donor')
                        else:
                            self.file_pairs.append(
                                {'donor': donor_file, 'recipient': db_file, 'similarity': (0, 0, 0), 'size': size})
                    else:
                        logger.debug('donor is same as recipient')

    def merge_files(self, recipient, donor):
        logger.info(' injecting \n%s \ninto \n%s', donor, recipient)

    def get_offsets(self, index):
        preceding_size = 0
        for entry in self.db_row_files:
            if entry['index'] < index:
                preceding_size += entry['size']

        file_offset = 0
        hash_offset = 0

        while preceding_size > self.db_block_size * hash_offset:
            hash_offset += 1
            file_offset += self.db_block_size
        file_offset = file_offset - preceding_size

        return(file_offset, hash_offset)

    def db_delete_row_with(self, db_conn, entry):
        query = 'delete from torrents '\
                'where id = (?) '
        self.cursor.execute(query, (entry['db_id'],))
        db_conn.commit()

    def fetch_row_for(self, entry):
        query = 'select id, torrent_id, name, category, content_layout, metadata from torrents '\
                'where id = (?) '
        self.cursor.execute(query, (entry['db_id'],))
        row = self.cursor.fetchall()[0]
        self.db_id = row[0]
        # print(self.db_id)
        # print(entry['db_id'])
        assert self.db_id == entry['db_id']
        self.db_torrent_id = row[1]
        self.db_name = row[2]
        self.db_category = row[3]
        self.db_layout = row[4]
        self.db_metadata = row[5]
        self.db_metainfo = bencode.bdecode(self.db_metadata)
        self.db_info = self.db_metainfo['info']
        self.db_pieces = io.BytesIO(self.db_info['pieces'])
        self.db_block_size = self.db_info['piece length']
        self.db_row_files = []
        idx = 0
        if 'files' in self.db_info:
            for entry in self.db_info['files']:
                if entry['length'] >= args.min_size:
                    self.db_row_files.append({'filename': entry['path'], 'size': entry['length'],
                                              'index': idx})
                idx += 1
        else:
            self.db_row_files.append({'filename': self.db_info['name'], 'size': self.db_info['length'],
                                      'index': 0})

    def compute_similarities(self, coverage):
        count = 0
        for pair in self.file_pairs:
            self.compute_similarity_pair(pair, coverage)
            count += 1

            print('pair {} of {}'. format(count, len(
                self.file_pairs)), end='          \r')

    def get_db_hash(self, index):
        self.db_pieces.seek(index * 20)
        hash = self.db_pieces.read(20)
        return hash

    def compute_similarity_pair(self, pair, coverage):

        try:
            fh1 = open(pair['donor'], 'rb')
        except FileNotFoundError:
            logger.warning('File not found\n%s', pair['donor'])

        if fh1:
            f2_entry = pair['recipient']
            self.fetch_row_for(f2_entry)
            f2_idx = f2_entry['db_tr_index']

            pair['similarity'] = self.compute_similarity(
                fh1, f2_idx, pair['size'], coverage)

            if pair['similarity'][1] < 20:
                print()
                pair['similarity'] = self.compute_similarity(
                    fh1, f2_idx, pair['size'], 0.05, autoadjust=False)

            if pair['similarity'][1] < 20:
                print()
                pair['similarity'] = self.compute_similarity(
                    fh1, f2_idx, pair['size'], 0.2, autoadjust=False)

            fh1.close()

        logger.debug('pair similarity %s\n%s\n%s',
                     pair['similarity'], pair['recipient'], pair['donor'])

    def compute_similarity(self, fh1, f2_idx, size, coverage, autoadjust=True):

        verified_blocks = 0
        non_zero_blocks = 0
        file_offset, hash_offset = self.get_offsets(f2_idx)
        size_fix = size - file_offset

        total_blocks = int(coverage * size_fix / self.db_block_size)

        while autoadjust and total_blocks > 50:
            coverage = coverage / 1.3
            total_blocks = int(coverage * size_fix / self.db_block_size)

        while autoadjust and total_blocks < 30:
            coverage = coverage * 1.3
            total_blocks = int(coverage * size_fix / self.db_block_size)

        for x in range(total_blocks):
            current_block = int(x/coverage)

            fh1.seek(file_offset + self.db_block_size * current_block)
            chunk = fh1.read(self.db_block_size)
            if chunk != bytes(len(chunk)):
                non_zero_blocks += 1
                chunk_hash = hashlib.sha1(chunk).digest()
                db_hash = self.get_db_hash(hash_offset + current_block)

                #logger.debug('file hash %s', chunk_hash)
                #logger.debug('db hash %s', db_hash)

                if chunk_hash == db_hash:
                    verified_blocks += 1
                else:
                    logger.debug('block %s of %s hash mismatch',
                                 x, total_blocks)

        return(verified_blocks, non_zero_blocks, total_blocks)

    def scan_row(self, row):
        self.db_id = row[0]
        #self.db_torrent_id = row[1]
        #self.db_name = row[2]
        #self.db_category = row[3]
        #self.db_layout = row[4]
        self.db_metadata = row[5]
        self.db_metainfo = bencode.bdecode(self.db_metadata)
        self.db_info = self.db_metainfo['info']
        #self.db_pieces = io.BytesIO(self.db_info['pieces'])
        self.db_block_size = self.db_info['piece length']

        idx = 0
        big_files = 0
        if 'files' in self.db_info:
            for entry in self.db_info['files']:
                if entry['length'] >= args.min_size:
                    big_files += 1

            if big_files <= args.max_files_in_torrent:
                for entry in self.db_info['files']:
                    if entry['length'] >= args.min_size:
                        self.db_all_files.add_entry(entry['path'], entry['length'],
                                                    self.db_id, idx)
                    idx += 1
            else:
                logger.debug('Too big torrent %s big files', big_files)
        else:
            self.db_all_files.add_entry(self.db_info['name'], self.db_info['length'],
                                        self.db_id, 0)

    def find_possible_deletees(self):
        deletees = []
        suspects = []

        if self.db_layout == 'NoSubfolder' and len(self.db_row_files) == 1:
            subfolders = ['']
        else:
            subfolders = ['', self.db_info['name']]

        if self.db_category:
            category_dirs = ['', self.db_category]
        else:
            category_dirs = ['']

        extensions = ['', '.!qb']

        for file in self.db_row_files:
            if isinstance(file['filename'], list):
                filename = ''
                for x in file['filename']:
                    filename = os.path.join(filename, x)
            else:
                filename = file['filename']

            size = file['size']
            for subfolder in subfolders:
                for category_dir in category_dirs:
                    for extension in extensions:
                        suspects.append({'size': size, 'filename':
                                         os.path.join(
                                             args.temp_dir, category_dir, subfolder, filename + extension)
                                         })
        #pprint.pprint(suspects)
        for suspect in suspects:
            if os.path.isfile(suspect['filename']):
                if os.lstat(suspect['filename']).st_size == suspect['size']:
                    deletees.append(suspect)
                else:
                    suspect['size'] = os.lstat(suspect['filename']).st_size
                    deletees.append(suspect)
                    logger.warning('file to delete size mismatch')
        return deletees


def main():

    sqlite_file_name = "n:\\torrents.db"
    try:
        db_conn = sqlite3.connect(sqlite_file_name)
    except:
        logger.critical('Db connection failed')
        sys.exit(1)

    cursor = db_conn.cursor()

    donors = args.dirs
    masstor = MassTorr(cursor, donors)
    masstor.scan_dirs()

    query = 'select id, torrent_id, name, category, content_layout, metadata from torrents '\
            'where metadata is not null'

    rows_scanned = 0
    print('scanning db')
    cursor.execute(query)
    row = cursor.fetchone()
    while row:
        print(f'rows scanned {rows_scanned}', end='      \r')
        masstor.scan_row(row)
        rows_scanned += 1
        row = cursor.fetchone()

    masstor.find_pairs()
    # pprint.pprint(masstor.file_pairs)
    masstor.compute_similarities(0.01)

    print()
    for pair in masstor.file_pairs:
        choice = ''
        masstor.fetch_row_for(pair['recipient'])
        delete_list = masstor.find_possible_deletees()
        while choice not in ['s', 'd']:
            print()
            print()
            print('size {} MiB pair similarity {}, recipient\n{}'.format(
                int(pair['size']*100/1024/1024)/100, pair['similarity'], pair['recipient']['filename']))

            print('Other files in the torrent')
            for file in masstor.db_row_files:
                print(file['index'], int(file['size']*100 /
                      1024/1024)/100, 'MiB', file['filename'])

            print('Possible to delete')
            for file in delete_list:

                print(int(file['size'] *100/1024/1024) /100, 
                'MiB', file['filename'])

            print()
            print('donor\n{}'.format(pair['donor']))
            print('(m)erge, (d)elete recipient, (r)ecalculate similarity, (s)kip')

            if args.skip_zero_sim and pair['similarity'][0] == 0:
                choice = 's'  # autoskip
                print('0 similiarities, autoskip')
            else:
                choice = input()
                if choice == 'r':
                    choice = input('% of file to check> ')
                    masstor.compute_similarity_pair(pair, int(choice)/100)
        if choice == 's':
            pass
        elif choice == 'd':
            masstor.db_delete_row_with(db_conn, pair['recipient'])
            for file in delete_list:
                os.remove(file['filename'])

        elif choice == 'm':
            # TODO
            pass


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

    parser.add_argument('temp_dir', type=str)

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

    parser.add_argument('--max_multi', type=int, default=5, dest='max_files_in_torrent',
                        help='Max number of big files in single torrent')

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
