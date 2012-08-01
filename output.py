import Queue
import socket
import json
import time
import datetime
import re
from threading import Thread
import sys
import os
import httplib
import logging

import warc

RESULTS = {
    'twitter-search.search': 'prepared_content.results',
    'youtube.search': 'feed.entry',
    'flickr.photos_search': 'photos.photo',
    'google_plus.activities_search': 'items',
    'facebook.search': 'data'
}

class ResponsesHandler: 

    def __init__(self): 
        self.triples_handler = TripleManager()
        self.warcs_handler = WARCManager()
        self.outlinks_handler = OutlinksManager()

    def add_response(self, response):
        # The whole response goes as a WARC
        self.warcs_handler.add(response)
        # Triples and outlinks are processed for each item of the response 
        result = self.manual_path(response)
        blender_config = response['blender_config']
        headers = response['headers']
        if type(result) is list:
            for _result in result:
                self.add_result(_result, blender_config, headers)
        else:
            self.add_result(result, blender_config, headers)

    def add_result(self, result, blender_config, headers):
        try:
            str(result['id'])
        except Exception:
            logging.error('Processing the output: %s has no ID' %\
                (result))
            return None
        init_outlinks = set()
        result_outlinks = list(self.extract_outlinks(result, init_outlinks))
        _clean_outlinks = self.clean_outlinks(result_outlinks)
        self.outlinks_handler.add(_clean_outlinks)
        self.triples_handler.add(result, blender_config, _clean_outlinks)

    def manual_path(self, response):
        result = response
        for item in RESULTS:
            found = False
            for key in RESULTS[item].split('.'):
                try:
                    result = result[key]
                    found = True
                except Exception:
                    found = False
                    break
            if found:
                return result

    def extract_outlinks(self, _result, outlinks):
        if type(_result) is dict:
            for key in _result.keys():
                self.extract_outlinks(_result[key], outlinks)
        elif type(_result) is list:
            for item in _result:
                self.extract_outlinks(item, outlinks)
        else:
            try:
                str_result = str(_result) 
            except Exception:
                return outlinks
            if re.match('https?://', str_result, re.I):
                outlinks.add(str(_result))
        return outlinks

    def clean_outlinks(self, outlinks):
        for outlink in outlinks:
            outlink = outlink.replace("&quot;",'')
            outlink = outlink.replace('"','')
            outlink = outlink.replace('\\','')
        return outlinks


class WARCManager:

    def __init__(self):
        self.open_warc()
        self.results_queue = Queue.Queue()
        self.start_daemon()

    def start_daemon(self):
        t = Thread(target=self.warcs_daemon)
        t.start() 

    def warcs_daemon(self): 
        while True:
            try:
                response = self.results_queue.get(False)
                self.write_warc(response)
            except Queue.Empty:
                continue

    def add(self, response):
        self.results_queue.put(response)

    def open_warc(self):
        warc_name = "apicrawler.%s.warc.gz" % (
            datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f'))
        logging.info("Writing warc file: %s" % warc_name)
        self.warc_file = warc.open("output/warcs/" + warc_name, "w")
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

    #TODO
    def write_warc(self, response):
        pass
#        try:
#            str(result['id'])
#        except Exception:
#            # TODO save problematic results
#            return None 
#        # Response record
#        target_uri = "http://%s%s" % (result['id']) 
#        warc_header = warc.WARCHeader({
#            "WARC-Type": "response",
#            "Content-Type": "application/json",
#            "WARC-Warcinfo-ID": self.warcinfo_id,
#            "WARC-Target-URI": target_uri,
#            "WARC-Identified-Payload-Type": "application/json"},\
#            defaults = True )
#        payload = json.dumps({ 'data': result, 'headers': headers })
#        fake_http_header = (" 200 OK\r\nContent-length: %d\r\n\r\n" %
#                            len(payload))
#        warc_payload = fake_http_header + payload
#        warc_record = warc.WARCRecord(warc_header, warc_payload)
#        self.warc_file.write_record(warc_record)
#        # Metadata record
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
#        if self.warc_file.tell() > 500 * 1024 * 1024:
#            self.close_warc()
#            self.open_warc()
#
    def close_warc(self):
        self.warc_file.close()
        # TODO use a temporary file name in open and rename here


class OutlinksManager:
    
    def __init__(self):
        self.outlinks_queue = Queue.Queue()
        self.start_daemon()

    def start_daemon(self):
        t = Thread(target=self.outlinks_daemon)
        t.start()

    def add(self, outlinks):
        for outlink in outlinks:
            self.outlinks_queue.put(outlink)

    def outlinks_daemon(self):
        outlinks = []
        while True:
            time.sleep(0.05)
            outlinks_at_a_time = 10
            while len(outlinks) < outlinks_at_a_time:
                try:
                    outlinks.append(self.outlinks_queue.get(False))
                except Queue.Empty:
                    continue
            try:
                self.send_outlinks(outlinks)
                prefix = 'output/outlinks/success.'
            except Exception:
                prefix = 'output/outlinks/failure.'
            with open(prefix + 'outlinks.txt', 'a') as outlinks_file:
                outlinks_file.write(json.dumps(outlinks) + '\n')
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
            logging.warning('Connexion with Heritrix broken')
        heritrix_connection.close()
        del outlinks_bulk[:]


class TripleManager:
    
    def __init__(self):
        self._triples = Queue.Queue()
        self.s = None
        self.start_daemon()
        self.current_filename = str(datetime.datetime.now()) + '.txt'

    def start_daemon(self):
        t = Thread(target=self.triples_daemon)
        t.start() 

    def triples_daemon(self): 
        # TODO replace time sleep with queue waiting
        while True:
            time.sleep(10)
            chunk = []
            quantity=1000000
            for i in range(0, quantity):
                try:
                    triple = self._triples.get(False)
                except Queue.Empty:
                    break
                chunk.append(triple)
            json.dump
            if not chunk:
                continue
            string_chunk = self.stringify_triples(chunk)
            logging.info('Sending %s' % string_chunk[0:75])
            prefix = 'apicrawler.socket-success.'
            try:
                self.initiate_socket_connection()
                self.s.send(string_chunk)
                self.close_socket()
            except Exception:
                prefix = 'apicrawler.socket-failure.'
            try:
                size = os.path.getsize('output/triples/' + prefix + \
                        self.current_filename)
            except Exception:
                size = 0
            if size > 500 * 1024 * 1024:
                self.current_filename = str(datetime.datetime.now())+'.txt'
            with open('output/triples/' + prefix + self.current_filename, 'a')\
            as _f:
                for triple in chunk:
                    _f.write(json.dumps(triple) + '\n')

   
    def add(self, result, blender_config, outlinks):
        try:
            triples = self.make_triples(result, blender_config, outlinks)
        except KeyError as e:
            logging.error(  'Weird data format for %s, error: %s' % \
                            (result,e))
        for triple in triples:
            self._triples.put(triple)

    def make_triples(self, result, blender_config, outlinks):
        triples = []
        api = '' 
        post = ['' for i in range(8)]
        from_user_subject = ''
        to_user_subjects = []
        users = []
        from_user = ['' for i in range(7)]
        #flickr
        if  (blender_config['server'], blender_config['interaction']) \
            == ('flickr', 'search'):
            api = 'flickr'
            #post
            post[0] = 'flickr/post/'  + str(result['id'])
            post[1] = result['id']
            post[2] = 'http://farm%s.staticflickr.com/%s/%s_%s.jpg' % \
                    (result['farm'], result['server'], result['id'], \
                    result['secret'])
            post[3] = result['title']
            #user
            from_user[0] = 'flickr/user/' + str(result['owner'])
            from_user_subject = from_user[0]
            from_user[1] = result['owner']
        #twitter
        if  (blender_config['server'], blender_config['interaction']) \
            == ('twitter-search', 'search'):
            api = 'twitter'
            #post
            post[0] = 'twitter/post/'  + str(result['id'])
            post[1] = result['id']
            post[2] = 'http://twitter.com/%s/status/%s' % \
                    (result['from_user'], result['id'])
            post[4] = result['text']
            post[5] = result['iso_language_code']
            post[6] = result['created_at']
            post[7] = result.get('geo', '')
            #from user
            from_user[0] = 'twitter/user/' + str(result['from_user_id'])
            from_user[1] = result['from_user_id']
            from_user[2] = 'http://twitter.com/%s/' % (result['from_user'])
            from_user[3] = result['from_user_name']
            from_user[4] = result['from_user']
            from_user[5] = result['profile_image_url']
            #to users
            for item in result['entities'].get('user_mentions', [{}]):
                if not item:
                    continue
                to_user = ['' for i in range(7)]
                to_user[0] = 'twitter/user/' + str(item['id'])
                to_user_subjects.append(to_user[0]) 
                to_user[1] = item['id']
                to_user[2] = 'http://twitter.com/%s/' % (item['screen_name'])
                to_user[3] = item['name']
                to_user[4] = item['screen_name']
                users.append( to_user )
        users.append(from_user)
        from_user_subject = from_user[0]
        #Post
        if not post[0]:
            return []
        triples.extend([
                [ post[0], 'api', api ],
                [ post[0], 'id', post[1] ],
                [ post[0], 'url', post[2] ],
                [ post[0], 'title', post[3] ],
                [ post[0], 'content', post[4] ],
                [ post[0], 'language', post[5] ],
                [ post[0], 'publication_date', post[6] ],
                [ post[0], 'location', post[7] ] ])
        # Outlinks
        for outlink in outlinks:
            triples.append([ post[0], 'outlink', outlink ])
        # Post from user / to users
        triples.append([ post[0], 'from_user', from_user_subject ])
        for to_user_subject in to_user_subjects:
            triples.append( [ post[0], 'to_user', to_user_subject ])
        # Users
        for user in users:
            if not user[0]:
                continue
            triples.extend([
                [ user[0], 'api', api ],
                [ user[0], 'id', user[1] ],
                [ user[0], 'url', user[2] ],
                [ user[0], 'name', user[3] ],
                [ user[0], 'nickname', user[4] ],
                [ user[0], 'picture_url', user[5] ],
                [ user[0], 'location', user[6] ] ])
        # Inverse relations, user has post / is mentionned
        triples.append([from_user_subject, 'has_post', post[0]])
        for to_user_subject in to_user_subjects:
            triples.append( [ to_user_subject, 'is_mentioned_by', post[0] ])
        triples = [triple for triple in triples if triple[2]]
        # TODO: harmonize publication_date (raw pub date et harm pub date)
        # TODO: harmonize location (raw location et harm location)
        # TODO: harmonize language (raw location et harm location)
        return triples

    def initiate_socket_connection(self, host='localhost', port=19898):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((host, port))

    def close_socket(self):
        self.s.close()

    def stringify_triples(self, triples):
        string_triples = []
        for triple in triples:
            string_triple = []
            for position, item  in enumerate(triple):
                if position < 2:
                    item = 'apicrawler:' + json.dumps(item)
                else:
                    item = json.dumps(item)
                string_triple.append(item.replace('"',''))
            string_triple = "\\SPO".join(string_triple)
            string_triples.append(string_triple)
        string_triples = "\\EOT".join(string_triples)
        string_triples += "\\EOL"
        return string_triples
