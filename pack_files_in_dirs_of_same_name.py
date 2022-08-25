#!/usr/bin/env python -u


import os
import shutil
VERSION = '0.0.1'

def main():
    root_path = args.dir
    for entry in os.scandir(root_path):
        if entry.is_file():
            print(entry.name)
            filename = entry.name
            for x in ['.!qb', '.mp4', '.wmv', '.avi', '.mkv']:
                if filename.lower().endswith(x):
                    filename = filename[:-4]

            print(filename)
            os.makedirs(os.path.join(root_path, filename))
            shutil.move(os.path.join(root_path, entry.name), \
                os.path.join(root_path, filename+'\\'))
 

if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(__name__ + '.txt')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)

    #logger.addHandler(fh)
    logger.addHandler(sh)

    from argparse import ArgumentParser
    parser = ArgumentParser(
        description='Post qBittorrent crash orphan restore')

    parser.add_argument('dir', type=str,
                        help='directory where pack all files to dirs .!qb files')


    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()
    
    main()
