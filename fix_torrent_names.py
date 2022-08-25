#!/usr/bin/env python -u

# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

#from fileinput import filename


import os
import sys
import shutil
import my_torrent_stuff

VERSION = "0.0.1"
TESTLEVEL = 0

def main():


    root_path = args.torrents_dir
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    a_torrent = my_torrent_stuff.Torrent()
    count_files = 0
    count_success = 0
    count_error = 0
    count_skip = 0

    for path, _, files in os.walk(root_path):
        for file in files:
            count_files+=1
            if file.endswith('.torrent'):
                a_torrent.load_torrent_file_info(os.path.join(root_path, file))
                if a_torrent.fl_name != file[:-len('.torrent')]:
                    try:
                        shutil.copy(os.path.join(root_path, file), os.path.join(args.output_dir, a_torrent.fl_name + '.torrent'))
                        print('copied', a_torrent.fl_name)
                        count_success+=1

                    except:
                        print('copy failed', a_torrent.fl_name)
                        count_error+=1

            else:
                #not a torrent
                count_skip+=1
    print('Files {}, renamed {}, errors {}, not torrents {}'\
                .format(count_files, count_success, count_error, count_skip))

if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARN)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(__name__ + '.txt')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(formatter)

    #logger.addHandler(fh)
    logger.addHandler(sh)

    from argparse import ArgumentParser
    parser = ArgumentParser(description='Process data from torrent files and push it to db')
    parser.add_argument('torrents_dir', help='dir with torrent files')
    parser.add_argument('output_dir', help='dir to copy renamed torrents')
    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()

    main()
