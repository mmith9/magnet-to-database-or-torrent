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

def merge():
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
    f1_size = os.stat(args.file1).st_size
    f2_size = os.stat(args.file2).st_size
    assert f1_size == f2_size

    print('merging:')
    print(args.file2)
    print('into')
    print(args.file1)

    fh1 = open(args.file1, 'r+b')
    fh2 = open(args.file2, 'rb')

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
            chunk1 = f1_buffer.pop(0)
            chunk2 = f2_buffer.pop(0)
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

            #elif chunk2 == bytes(actual_size):  # is all zeros

#            fh1.seek((chunks_num - 1) * chunk_size)
#            fh1.write(new_chunk)
#            merged += 1

        if (chunks_num % 100) == 0:
            if args.stats:
                print('\rProgress chunks: {}={}%, merged {}={}% , f1 empty {}, f2 empty {}, same non 0 {}, diff and non 0 {}'.format(
                    chunks_num, int(chunks_num*chunk_size/f1_size*100),
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

def main():
    merge()
    if args.both_ways:
        args.file1, args.file2 = (args.file2, args.file1)
        merge()
    input()

if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.ERROR)
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
        description='Merge into all .!qb in partial torrent dir, from all dirs, torrent temp included')
        
    parser.add_argument('dirs', type=str, nargs='+')
   

    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()

    print(args)