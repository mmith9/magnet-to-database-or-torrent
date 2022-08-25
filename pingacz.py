#!/usr/bin/env python -u

import time
import threading
import queue
import icmplib

class Pingacz(threading.Thread):
    def __init__(self, thread_id, name, lock, queue):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = name
        self.lock = lock
        self.queue = queue
        self.number = 0

    def run(self):

        while True:
            time.sleep(1)
            self.queue.put(self.number)
            self.number+=1
            response = icmplib.ping('8.8.8.8')
            self.queue.put(response)
            

def main():
    lock = threading.Lock()
    a_queue = queue.Queue()
    a_thread = Pingacz(1, 'just a name', lock, a_queue)
    a_thread.start()

    while True:

        print('.', end='')
        time.sleep(0.1)
        if not a_queue.empty():
            print(a_queue.get(), end='')

main()
