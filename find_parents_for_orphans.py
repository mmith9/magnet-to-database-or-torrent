#!/usr/bin/env python -u

import os
import shutil
VERSION = '0.0.1'

def main():
    for entry in os.scandir(args.orphans_dir):
        suspects = []
        suspects.append(os.path.join(args.torrents_dir, entry.name+'.torrent'))
        tr_name = entry.name
        if tr_name.find('.') > 0:
            tr_name = tr_name[:tr_name.find('.')]
            suspects.append(os.path.join(args.torrents_dir, tr_name+'.torrent'))

        for x in ['.mp4', '.wmv', '.avi', '.mkv']:
            suspects.append(os.path.join(args.torrents_dir, entry.name + x + '.torrent'))
        for suspect in suspects:
            #print(suspect)
            if os.path.isfile(suspect):
                try:
                    shutil.copy(suspect, args.outputdir)
                    print(suspect)
                except:
                    print('copy fail of:', suspect)


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

    parser.add_argument('orphans_dir', type=str,
                        help='directory to look for orphans - dirs and single .!qb files')
    parser.add_argument('torrents_dir', type=str,
                        help='dir with .torrent files library')
    parser.add_argument('-o', '--outputdir', dest='outputdir', type=str, default='.',
                        help='dir to copy found .torrent files')

    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()
    
    main()
