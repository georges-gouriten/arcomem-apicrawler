import logging
import Queue
from threading import Thread
import datetime
import time
import os

import warc

import config

logger = logging.getLogger('warcs')

class WARCManager:
    def __init__(self):
        self.warcinfo_id = None
        self.responses_queue = Queue.Queue()
        self.start_daemons()
        logger.info('WARCs Manager started')
        self.open_warc()
        self.response_counter = 0

    def start_daemons(self):
        warcs_daemon_thread = Thread(target=self.warcs_daemon)
        warcs_daemon_thread.start() 
        writing_rate_thread = Thread(target=self.writing_rate_daemon)
        writing_rate_thread.start() 

    def writing_rate_daemon(self):
        """ Periodically reports on the WARCs writing rate """
        _period = config.warcs_rate_period
        while True:
            time.sleep(_period)
            logger.info('In the last %d s, I processed %d responses' %
                    (_period, self.response_counter))
            self.response_counter = 0

    def warcs_daemon(self): 
        """ Looks into the response queue and writes it in the WARC """
        while True:
            # If the queue is empty, it will wait till a new response is
            # added to the queue
            response = self.responses_queue.get(True)
            self.write_warc(response)

    def add_response(self, response):
        self.responses_queue.put(response)

    def open_warc(self):
        self.warc_file_path = os.path.join (
                config.warcs_path,
                "apicrawler.%s.warc.gz" % (
                datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')))
                
        logger.info("Writing new WARC file: %s" % self.warc_file_path)
        self.warc_file = warc.open(self.warc_file_path, "w")
        #
        # Write WARCInfo record
        #
        warc_header = warc.WARCHeader(
                {   "WARC-Type": "warcinfo",
                    "Content-Type": "application/warc-fields",
                    "WARC-Filename": os.path.basename(self.warc_file_path)  },
                defaults = True)
        warc_payload = 'software: apicrawler\nhostname: ia200127'
        warc_record = warc.WARCRecord(warc_header, warc_payload)
        self.warc_file.write_record(warc_record)
        self.warcinfo_id = warc_header['WARC-RECORD-ID']
        logger.info("New WARC id: %s" % self.warcinfo_id)

    def write_warc(self, response):
        #
        # Write response record
        #
        target_uri = response['blender_config']['request_url'] 
        warc_header = warc.WARCHeader(
            {   
                "WARC-Type": "response",
                "Content-Type": "application/json",
                "WARC-Warcinfo-ID": self.warcinfo_id,
                "WARC-Target-URI": target_uri,
                "WARC-Identified-Payload-Type": "application/json"  
            },
            defaults = True )
        payload = response['raw_content']
        fake_http_header = (" 200 OK\r\nContent-length: %d\r\n\r\n" %
                            len(payload))
        warc_payload = '%s%s' % (fake_http_header, payload)
        warc_record = warc.WARCRecord(warc_header, warc_payload)
        self.warc_file.write_record(warc_record)
#
#
#       # Write metadata record (Deprecated)
#
#       It could be used for outlinks but it is not a requirement at the
#       moment, outlinks are already sent to the crawler and to the triple
#       store.
#
#
#        concurrent_to = warc_header['WARC-RECORD-ID']
#        warc_header = warc.WARCHeader({
#            "WARC-Type": "metadata",
#            "Content-Type": "application/warc-fields",
#            "WARC-Warcinfo-ID": self.warcinfo_id,
#            "WARC-Target-URI": target_uri,
#            "WARC-Concurrent-To": concurrent_to,
#            "WARC-Identified-Payload-Type": "application/json"},\
#            defaults = True )
#        warc_payload = json.dumps({"parent": str(target_uri),
#                                   "outlinks": outlinks})
#        warc_record = warc.WARCRecord(warc_header, warc_payload)
#        self.warc_file.write_record(warc_record)
#
#
        if self.warc_file.tell() > 500 * 1024 * 1024:
            self.close_warc()
            self.open_warc()
        self.response_counter += 1

    def close_warc(self):
        self.warc_file.close()
