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
        self.responses_queue = Queue.Queue()
        self.start_daemons()
        logger.warning('WARC Manager started')
        self.open_warc()
        self.response_counter = 0

    def start_daemons(self):
        warcs_daemon_thread = Thread(target=self.warcs_daemon)
        warcs_daemon_thread.start() 
        writing_rate_thread = Thread(target=self.writing_rate_daemon)
        writing_rate_thread.start() 

    def writing_rate_daemon(self):
        _period=100
        while True:
            time.sleep(_period)
            logger.warning('In the last %d s, I processed %d responses' %
                    (_period, self.response_counter))
            self.response_counter = 0

    def warcs_daemon(self): 
        while True:
            try:
                response = self.responses_queue.get(False)
                self.write_warc(response)
            except Queue.Empty:
                continue

    def add_response(self, response):
        self.responses_queue.put(response)

    def open_warc(self):
        warc_name = "apicrawler.%s.warc.gz" % (
            datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f'))
        logger.warning("Writing new warc file: %s" % warc_name)
        self.warc_file = warc.open( os.path.join(
                                        config.warcs_path, 
                                        warc_name)
                                    , "w")
        # WARCInfo record
        warc_header = warc.WARCHeader({
                        "WARC-Type": "warcinfo",
                        "Content-Type": "application/warc-fields",
                        "WARC-Filename": warc_name},\
                        defaults = True)
        warc_payload = 'software: apicrawler\nhostname: ia200127'
        warc_record = warc.WARCRecord(warc_header, warc_payload)
        self.warc_file.write_record(warc_record)
        self.warcinfo_id = warc_header['WARC-RECORD-ID']

    def write_warc(self, response):
        # Response record
        target_uri = response['blender_config']['request_url'] 
        warc_header = warc.WARCHeader(
            {   
                "WARC-Type": "response",
                # Only json at the moment
                "Content-Type": "application/json",
                "WARC-Warcinfo-ID": self.warcinfo_id,
                "WARC-Target-URI": target_uri,
                "WARC-Identified-Payload-Type": "application/json"  
            },
            defaults = True )
        payload = response['raw_content']
        fake_http_header = (" 200 OK\r\nContent-length: %d\r\n\r\n" %
                            len(payload))
        warc_payload = fake_http_header + payload
        warc_record = warc.WARCRecord(warc_header, warc_payload)
        self.warc_file.write_record(warc_record)
#        # Metadata record (currently not used)
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
        if self.warc_file.tell() > 500 * 1024 * 1024:
            self.close_warc()
            self.open_warc()
        self.response_counter += 1

    def close_warc(self):
        self.warc_file.close()
