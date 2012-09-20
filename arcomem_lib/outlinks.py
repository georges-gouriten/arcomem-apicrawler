import logging
import Queue
import httplib
import json
from threading import Thread
import datetime
import os

import config

logger = logging.getLogger('outlinks')
#
# IDEA: Could have a backup file change mecanism (like for outlinks), see
# if this is relevant
#
class OutlinksManager:
    """ Extracts outlinks from API responses and sends them to the crawler
    or writes it into a backup file """
    def __init__(self):
        self.outlinks_queue = Queue.Queue()
        self.start_daemon()
        logger.info('Outlinks Manager started')
        # The backup file is used for outlinks that where not successfully
        # transferred to the crawler
        #
        # IDEA: we could have a mecanism similar to the one for the triple
        # store with a changing file name when it gets too big.
        #
        self.backup_file = os.path.join(   config.outlinks_path, 
                                      'backup.outlinks.txt') 
        logger.info('Backup file: %s' % self.backup_file)

    def start_daemon(self):
        t = Thread(target=self.outlinks_daemon)
        t.start()

    def add_outlinks(self, outlinks):
        """ Adds outlinks to the queue """
        for outlink in outlinks:
            self.outlinks_queue.put(outlink)

    def outlinks_daemon(self):
        """ Loops and takes care of outlinks in the queue """
        outlinks_chunk = []
        chunk_size = config.outlinks_chunk_size
        save_backup = False
        while True:
            # Waits for the chunk to be full
            while len(outlinks_chunk) < chunk_size:
                outlinks_chunk.append(self.outlinks_queue.get(True))
            # Sends the chunk
            logger.info('[In progress] Sending %s outlinks to the crawler'\
                        % chunk_size)
            try:
                # TODO: sends to heritrix so far, IMF crawler?
                self.send_outlinks_to_heritrix(outlinks_chunk)
                logger.warning('[Success] Sent %s outlinks to the crawler'\
                        % chunk_size)
            except Exception as e:
                logger.warning('[Failure] Exception occured during an '
                               'attempt to send the outlinks: %s' % e)
                save_backup = True
            if save_backup:
                logger.info('[In progress] Saving outlinks to backup file')
                with open(self.backup_file, 'a') as _backup_file:
                    _backup_file.write( ' ** Backup **\nDate: ' + \
                    datetime.datetime.now().strftime(config.datetime_format)\
                    + '\n')
                    _backup_file.write(json.dumps(outlinks_chunk) + '\n')
                logger.info('[Success] Saved outlinks to backup file')
            # Cleans variables
            del outlinks_chunk[:]
            save_backup = False

    def send_outlinks_to_heritrix(self, outlinks):
        """ Sends outlinks to Heritrix crawler on machine ia200127 """
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
            raise Exception, 'Wrong status code'
        heritrix_connection.close()
        del outlinks_bulk[:]
