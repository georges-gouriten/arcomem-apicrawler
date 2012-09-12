import logging
import Queue
import httplib
import json
from threading import Thread
import datetime
import os

import config

logger = logging.getLogger('outlinks')

class OutlinksManager:
    def __init__(self):
        self.outlinks_queue = Queue.Queue()
        self.start_daemon()

    def start_daemon(self):
        t = Thread(target=self.outlinks_daemon)
        t.start()

    def add_outlinks(self, outlinks):
        for outlink in outlinks:
            self.outlinks_queue.put(outlink)

    def outlinks_daemon(self):
        outlinks = []
        while True:
            #time.sleep(0.05)
            outlinks_at_a_time = 10
            while len(outlinks) < outlinks_at_a_time:
                try:
                    outlinks.append(self.outlinks_queue.get(False))
                except Queue.Empty:
                    continue
            try:
                self.send_outlinks(outlinks)
                prefix = 'success.'
            except Exception:
                prefix = 'failure.'
            outlinks_file = os.path.join(   config.outlinks_path, 
                                            prefix + 'outlinks.txt') 
            with open(outlinks_file, 'a') as _outlinks_file:
                _outlinks_file.write(json.dumps(outlinks) + '\n')
            del outlinks[:]

    def send_outlinks(self, outlinks):
        heritrix_connection = httplib.HTTPConnection( \
                'ia200127.eu.archive.org', 8080, timeout=0.1 )
        heritrix_connection.connect()
        outlinks_bulk = []
        for outlink in outlinks:
            outlinks_bulk.append({"url": outlink, "score": 1.0})
        heritrix_headers = {    'Content-Type': 'text/json',
                                'charset': 'utf-8'  }
        heritrix_connection.request('POST', 'queue/update/', \
                        body=json.dumps(outlinks_bulk), \
                        headers=heritrix_headers)
        response = heritrix_connection.getresponse()
        if response.status <> 200:
            logger.warning('Connexion with Heritrix broken')
        heritrix_connection.close()
        del outlinks_bulk[:]
