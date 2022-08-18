#!/usr/bin/env python -u

import os
import sys
import time
import binascii
from typing import List
import random
import tempfile
import mysql.connector
import libtorrent
import my_torrent_stuff
from pprint import PrettyPrinter

VERSION = "0.0.1"
pp = PrettyPrinter()

class Counter:
    def __init__(self) -> None:
        self.names = ['new', 'old', 'resolved', 'offloaded']
        self.values = {}
        for name in self.names:
            self.values[name] = 0

    def increase(self, what: str, value: int):
        self.values[what] += value

    def value_of(self, what: str) -> int:
        return self.values[what]

class Job:
    def __init__(self):
        self.id = 0
        self.handle = None
        self.total_runtime = 0
        self.session_runtime = 0
        self.hexhash: str

    def is_complete(self):
        ret = True
        status = self.handle.status()
        ret = ret and status.has_metadata
#        ret = ret and status.last_seen_complete > 0
#        ret = ret and status.list_seeds > 0
#        ret = ret and status.list_peers > 0
        return ret

    def has_meta(self):
        status = self.handle.status()
        return status.has_metadata

    def is_timeout(self):
        status = self.handle.status()
        return status.active_time > self.session_runtime + args.timeout

    def is_aged(self):
        status = self.handle.status()
        return (status.active_time) > args.aged

    def just_die(self, session_handle):
        status = self.handle.status()
        self.session_runtime = status.active_time
        session_handle.remove_torrent(self.handle)
        return

    def reap_data(self):
        output = {}
        logger.debug('reaping completed job')
        status = self.handle.status()
        output['id'] = self.id
        output['name'] = status.name
        output['last_seen_complete'] = status.last_seen_complete
        output['peers'] = status.list_peers
        output['seeds'] = status.list_seeds
        output['hexhash'] = self.hexhash

        torinfo = self.handle.torrent_file()
        files = torinfo.files()
        output['num_files'] = files.num_files()
        output['file_list'] = []
        for i in range(files.num_files()):
            output['file_list'].append(
                [files.file_path(i), files.file_size(i)])
        return output

    def reap_torrent_file(self):
        step1 = self.handle.status()
        step2 = step1.torrent_file
        step3 = libtorrent.create_torrent(step2)
        step4 = step3.generate()
        step5 = libtorrent.bencode(step4)
        return step5

    def go_to_sleep(self):
        logger.debug('Job going to sleep')
        self.handle.pause()

    def wake_up(self):
        logger.debug('Job waking up')
        self.handle.resume()
        status = self.handle.status()
        self.session_runtime = status.active_time

class Trackers:
    def __init__(self) -> None:
        self.list = []
        self.num = 0

    def load_from_file(self, filename:str) -> None:
        f_h = open(filename, "r", encoding='utf-8')
        trackers = f_h.readlines()
        f_h.close()
        for row in trackers:
            self.list.append({'url':row.strip('\n'), 'uses':0, 'resolves':0, 'ratio':0})
        self.num = len(self.list)

    def load_from_db(self, cursor):
        query = 'select url, uses, resolves, ratio from trackers'
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            self.list.append({'url':row[0], 'uses':row[1], 'resolves':row[2], 'ratio':row[3]})
        self.num = len(self.list)

    def save_to_db(self, cursor):
        query_check = 'select count(1) from trackers '\
                        'where url like (%s) '

        query_update = 'update trackers set uses = (%s), resolves = (%s), ratio = (%s) '\
                        'where url like (%s) '

        query_insert = 'insert into trackers (uses, resolves, ratio, url) '\
                        'values (%s, %s, %s, %s) '

        for tracker in self.list:
            cursor.execute(query_check, (tracker['url'],))
            row = cursor.fetchone()
            if row[0] > 0:
                cursor.execute(query_update, \
                    (tracker['uses'], tracker['resolves'], tracker['ratio'], tracker['url']))
            else:
                print(tracker)
                cursor.execute(query_insert, \
                    (tracker['uses'], tracker['resolves'], tracker['ratio'], tracker['url']))

    def get_random_url(self, amount:int) -> list:
        if amount < self.num:
            trackers = random.sample(self.list, amount)
        else:
            trackers = self.list

        url_list = []
        for tracker in trackers:
#            tracker['uses'] +=1
            url_list.append(tracker['url'])

        return url_list

    def report_success(self, job:Job) ->None:
        trackers = job.handle.trackers()
        urls = []
        for tracker in trackers:
            urls.append(tracker['url'])
        for tracker in self.list:
            if tracker['url'] in urls:
                tracker['resolves'] += 1
                tracker['uses'] +=1
                tracker['ratio'] = tracker['resolves'] / tracker['uses']

    def report_failure(self, job:Job) ->None:
        trackers = job.handle.trackers()
        urls = []
        for tracker in trackers:
            urls.append(tracker['url'])
        for tracker in self.list:
            if tracker['url'] in urls:
                tracker['uses'] +=1
                tracker['ratio'] = tracker['resolves'] / tracker['uses']


class Resolver:
    def __init__(self) -> None:
        logger.debug('object initialization')
        self.jobs: List[Job]
        self.timeout_jobs: List[Job]
        self.jobs = []
        self.timeout_jobs = []
        self.trackers: List[str]
        self.trackers = []
        self.hashes_to_resolve = []
        self.count = Counter()
        logger.debug('connecting to db')
        mysql_database_name = "tpb"
        mysql_user = os.environ.get("mysql_user")
        mysql_password = os.environ.get("mysql_password")
        self.trackers = Trackers()
        try:
            self.conn = mysql.connector.connect(
                host='localhost',
                user=mysql_user,
                password=mysql_password,
                database=mysql_database_name
            )
            self.cursor = self.conn.cursor()
        except mysql.connector.Error:
            print('Connection to db failed')
            sys.exit()

        logger.debug('initializing libtorrent session')
        self.lt_session = libtorrent.session()
        self.lt_params = libtorrent.add_torrent_params()
        ltflags = libtorrent.add_torrent_params_flags_t

        self.lt_params.flags &= ~ ltflags.flag_auto_managed
        self.lt_params.flags |= ltflags.flag_upload_mode

        # necessary as upload-only does not prevent creation of
        # empty files and directory structure under them
        self.tmpdir = tempfile.TemporaryDirectory()
        self.lt_params.save_path = self.tmpdir.name
        self.lt_params.storage_mode = libtorrent.storage_mode_t(2)
        self.lt_params.max_connections = 2

    def get_trackers(self):
        #self.trackers.load_from_file('trackerlist.txt')
        #self.save_trackers()
        self.trackers.load_from_db(self.cursor)

    def save_trackers(self):
        self.trackers.save_to_db(self.cursor)
        self.conn.commit()

    def get_new_jobs(self) -> int:  # number of new jobs
        logger.debug('loading new jobs')
        query = 'select id, infohash from hashes_to_resolve '\
                'order by id '\
                'limit %s'

        self.cursor.execute(query, (args.maxnew,))
        rows = self.cursor.fetchall()
        for row in rows:
            self.hashes_to_resolve.append([row[0], row[1], 0, 0])
        self.count.increase('new', len(rows))
        return

    def get_old_jobs(self) -> int:
        logger.debug('Loading old jobs')
        query = 'select id, infohash, runtime from old_hashes_to_resolve '\
                'order by runtime asc '\
                'limit %s'
        self.cursor.execute(query, (args.maxold,))
        rows = self.cursor.fetchall()
        for row in rows:
            self.hashes_to_resolve.append([row[0], row[1], row[2], 0])
        self.count.increase('old', len(rows))
        return

    def sort_jobs(self):
        pass
        # sorting actually done in select preparing hashes in db
        #logger.warning("Sorting not implemented yet")

    def spawn_a_job(self):
        logger.debug('spawning a job')
        job = Job()
        job.id, job.hexhash, job.total_runtime, job.session_runtime = \
            self.hashes_to_resolve.pop(0)
        self.lt_params.info_hash = libtorrent.sha1_hash(
            binascii.a2b_hex(job.hexhash))
        self.lt_params.name = "name_" + job.hexhash
        self.lt_params.trackers = self.trackers.get_random_url(3)
        job.handle = self.lt_session.add_torrent(self.lt_params)

        job.handle.resume()
        self.jobs.append(job)
        _cur_trackers = job.handle.trackers()
        logger.debug('hashes %s, running %s, sleeping %s',
                     len(self.hashes_to_resolve), len(self.jobs), len(self.timeout_jobs))

    def end_a_job(self, job):
        logger.debug('Removing a job completely')
        job.just_die(self.lt_session)

        if job.total_runtime == 0:  # a new hash
            query = 'delete from hashes_to_resolve '\
                    'where id = (%s) and infohash like (%s) '
        else:
            query = 'delete from old_hashes_to_resolve '\
                    'where id = (%s) and infohash like (%s) '

        self.cursor.execute(query, (job.id, job.hexhash))
        self.conn.commit()
        self.jobs.remove(job)

    def enqueue_a_job(self, job: Job):
        logger.debug('timed out job -> back to queueto the end of queue')
        job.go_to_sleep()
        self.jobs.remove(job)
        self.timeout_jobs.append(job)

    def wake_a_job(self):
        logger.debug('Waking up a timed out(previously) job')
        job = self.timeout_jobs.pop(0)
        job.wake_up()
        self.jobs.append(job)
        logger.debug('hashes %s, running %s, sleeping %s',
                     len(self.hashes_to_resolve), len(self.jobs), len(self.timeout_jobs))

    def offload_aged_job(self, job: Job):
        logger.debug('offloading aged job to db')
        job.just_die(self.lt_session)
        if job.total_runtime == 0:  # It was a new hash
            query = 'delete from hashes_to_resolve '\
                    'where id = (%s) and infohash like (%s) '
            self.cursor.execute(query, (job.id, job.hexhash))

            query = 'insert into old_hashes_to_resolve '\
                    '(id, infohash, runtime) '\
                    'values (%s, %s, %s)'
            self.cursor.execute(
                query, (job.id, job.hexhash, job.session_runtime))

        else:  # it was an old hash
            job.total_runtime += job.session_runtime
            query = 'update old_hashes_to_resolve '\
                    'set runtime = (%s) '\
                    'where id = (%s) and infohash like (%s) '
            self.cursor.execute(
                query, (job.total_runtime, job.id, job.hexhash))

        self.conn.commit()
        self.jobs.remove(job)
        self.count.increase('offloaded', 1)

    def push_resolved_hash_to_db(self, output):
        assert False
        query = 'insert into resolved_hashes '\
                '(id, infohash, name, peers, seeds, last_seen_complete, num_files) '\
                'values (%s, %s, %s, %s, %s, %s, %s) '
        data_row = (output['id'],
                    output['hexhash'],
                    output['name'],
                    output['peers'],
                    output['seeds'],
                    output['last_seen_complete'],
                    output['num_files']
                    )
        self.cursor.execute(query, data_row)
        self.conn.commit()
        if output['num_files'] > 1:
            query = 'insert into resolved_files (name, size, parenttorrentid) '\
                    'values (%s, %s, %s) '
            for file in output['file_list']:
                self.cursor.execute(query, (file[0], file[1], output['id']))
        self.conn.commit()
        self.count.increase('resolved', 1)

    def print_stats_inline(self):
        print('new {} old, {}, resolved {}, queue {}, active {}, sleep {}, offloaded {}    \r'\
            .format(self.count.value_of('new'), self.count.value_of('old'), \
                    self.count.value_of('resolved'), len(self.hashes_to_resolve), \
                    len(self.jobs), len(self.timeout_jobs), self.count.value_of('offloaded')\
            ), end='')

    def run_loop(self):
        """ Main part of program, once initialized runs indefinitely
            or until jobs are done (unlikely)
        """
        while self.jobs or self.hashes_to_resolve or self.timeout_jobs:
            while (len(self.jobs) < args.threads) and (self.hashes_to_resolve or self.timeout_jobs):
                if self.hashes_to_resolve:
                    self.spawn_a_job()
                else:
                    self.wake_a_job()
                self.print_stats_inline()
                time.sleep(args.spawn)

            time.sleep(10)

            for job in reversed(self.jobs):
                if job.is_complete():
                    #output = job.reap_data()
                    # self.push_resolved_hash_to_db(output)

                    a_torrent = my_torrent_stuff.Torrent()
                    a_torrent.torfile = job.reap_torrent_file()
                    a_torrent.digest_torfile()
                    assert a_torrent.fl_hexhash.lower() == job.hexhash.lower()
                    a_torrent.get_db_info(self.cursor, dbtype='mysql')
                    a_torrent.update_db(self.cursor, dbtype='mysql')

                    if args.torrents_dir:
                        a_torrent.save_file_to(args.torrents_dir)
                    self.trackers.report_success(job)
                    self.end_a_job(job)
                    self.count.increase('resolved', 1)
                    logger.debug('Saved resolved job')

                elif job.is_timeout():
                    self.enqueue_a_job(job)
                elif job.is_aged():
                    self.trackers.report_failure(job)
                    self.offload_aged_job(job)

                self.print_stats_inline()
                time.sleep(args.spawn)

            logger.info('new %s, old %s, resolved %s, offloaded %s',
                        self.count.value_of('new'), self.count.value_of('old'),
                        self.count.value_of('resolved'), self.count.value_of('offloaded'))
            logger.info('trackers %s, hashes %s, running %s, sleeping %s',
                        self.trackers.num, 
                        len(self.hashes_to_resolve), len(self.jobs), len(self.timeout_jobs))
            self.print_stats_inline()

        logger.info('No more jobs')


def main():
    resolver = Resolver()
    while True:
        resolver.get_trackers()
        resolver.get_new_jobs()
        resolver.get_old_jobs()
        resolver.sort_jobs()
        resolver.run_loop()
        logger.info('Cycle complete, trying to get new jobs')
        resolver.save_trackers()
        old_resolver = resolver
        old_resolver.lt_session.pause()
        resolver = Resolver()
        resolver.count = old_resolver.count
        time.sleep(30)
        del old_resolver


if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(__name__ + '.txt')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)

    from argparse import ArgumentParser
    parser = ArgumentParser(
        description='Unknown/incomplete info hash resolver')
    # parser.add_argument('hash', help='a hash to push to db')
    # parser.add_argument('-f', '--file', action='store_true', dest='file',\
    #  help='Use file with hashes instead, one per line')
    # parser.add_argument('-d', '--dir', action='store_true', dest='dir', \
    # help='Directory with .torrent files')

    parser.add_argument('-d', '-dir', dest='torrents_dir', default='', type=str,
                        help='directory to save torrent files, default don\'t save')
    parser.add_argument('-timeout', dest='timeout', default=50, type=int,
                        help='timeout in seconds for single try of hash')
    parser.add_argument('-aged', dest='aged', default=80, type=int,
                        help='timeout in seconds before offload back to db')
    parser.add_argument('-threads', dest='threads', default=200, type=int,
                        help='maximum concurrent hashes')
    parser.add_argument('-spawntime', dest='spawn', default=100, type=int,
                        help='min time between torrent spawns in miliseconds')
    parser.add_argument('-maxnew', dest='maxnew', default=1000, type=int,
                        help='maximum new hashes at once')
    parser.add_argument('-maxold', dest='maxold', default=100, type=int,
                        help='maximum old hashes at once')

    parser.add_argument('--version', action='version', version=VERSION)
    args = parser.parse_args()
    args.spawn = args.spawn / 1000

    main()
