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

RESPONSE_CONTENTS = {
    'twitter-search.search': 'results',
    'youtube.search': 'feed.entry',
    'flickr.photos_search': 'photos.photo',
    'google_plus.activities_search': 'items',
    'facebook.search': 'data'
}

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "output")
TRIPLES_PATH = os.path.join(OUTPUT_PATH, 'triples')
OUTLINKS_PATH = os.path.join(OUTPUT_PATH, 'outlinks')
WARCS_PATH = os.path.join(OUTPUT_PATH, 'warcs')

class ResponsesHandler: 
    """ Handles blender responses """
    def __init__(self): 
        self.triples_handler = TripleManager()
        self.warcs_handler = WARCManager()
        self.outlinks_handler = OutlinksManager()

    def add_response(self, response):
        # The whole response goes as a WARC
        self.warcs_handler.add_response(response)
        # Triples and outlinks are processed for each item of the response 
        # Manually finds the content in the response
        blender_content = response['loaded_content']
        response_content = self.find_response_content(blender_content)
        blender_config = response['blender_config']
        headers = response['headers']
        # From response content to content item (e.g., from a set of tweets
        # to a unique tweet) 
        if type(response_content) is list:
            for content_item in response_content:
                self.add_content_item(  content_item, 
                                        blender_config, 
                                        headers )
        else:
            self.add_content_item(response_content, blender_config, headers)

    def add_content_item(self, content_item, blender_config, headers):
        try:
            str(content_item['id'])
        except Exception:
            logging.error('Processing the output: %s has no ID' %\
                (content_item))
            return None
        init_outlinks = set()
        content_item_outlinks = list(self.extract_outlinks( content_item, 
                                                            init_outlinks))
        _clean_outlinks = set(self.clean_outlinks(content_item_outlinks))
        all_outlinks = \
            self.triples_handler.add_content_item(   content_item, 
                                                blender_config, 
                                                _clean_outlinks )
        self.outlinks_handler.add_outlinks(all_outlinks)

    def find_response_content(self, response):
        response_content = response
        for item in RESPONSE_CONTENTS:
            found = False
            for key in RESPONSE_CONTENTS[item].split('.'):
                try:
                    response_content = response_content[key]
                    found = True
                except Exception:
                    found = False
                    break
            if found:
                return response_content

    def extract_outlinks(self, _content, outlinks):
        if type(_content) is dict:
            for key in _content.keys():
                self.extract_outlinks(_content[key], outlinks)
        elif type(_content) is list:
            for item in _content:
                self.extract_outlinks(item, outlinks)
        else:
            try:
                str_content = str(_content) 
            except Exception:
                return outlinks
            if re.match('https?://', str_content, re.I):
                outlinks.add(str(_content))
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
        self.responses_queue = Queue.Queue()
        self.start_daemon()

    def start_daemon(self):
        t = Thread(target=self.warcs_daemon)
        t.start() 

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
        logging.info("Writing warc file: %s" % warc_name)
        self.warc_file = warc.open(os.path.join(WARCS_PATH, warc_name), "w")
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
        pass
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

    def close_warc(self):
        self.warc_file.close()


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
            outlinks_file = os.path.join(TRIPLES_PATH, prefix+'outlinks.txt') 
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
        while True:
            chunk = []
            quantity=100000
            for i in range(0, quantity):
                triple = self._triples.get(True)
                chunk.append(triple)
            json.dump
            if not chunk:
                continue
            string_chunk = self.stringify_triples(chunk)
            logging.info('Sending %s' % string_chunk[0:75])
            prefix = 'apicrawler.socket-success.'
            try:
#               # Deprecated triple store socket communication
#               self.initiate_socket_connection()
#               self.s.send(string_chunk)
#               self.close_socket()
                raise NotImplementedError, 'waiting for Nikos' 
            except Exception:
                prefix = 'apicrawler.socket-failure.'
            triple_file = os.path.join( TRIPLES_PATH, 
                                        prefix + self.current_filename)
            try:
                size = os.path.getsize(triple_file)
            except OSError:
                size = 0
            if size > 500 * 1024 * 1024:
                self.current_filename = str(datetime.datetime.now())+'.txt'
                triple_file = os.path.join( TRIPLES_PATH, 
                                            prefix + self.current_filename)
            with open(triple_file, 'a') as _f:
                for triple in chunk:
                    _f.write(json.dumps(triple) + '\n')

   
    def add_content_item(self, content_item, blender_config, outlinks):
        triples = []
        new_outlinks = set()
        try:
            triples, new_outlinks = \
                    self.make_triples(content_item, blender_config, outlinks)
        except Exception as e:
            logging.error(  'Weird data format for %s, error: %s' % \
                            (content_item,e))
        for triple in triples:
            self._triples.put(triple)
        return outlinks.union(new_outlinks)

    def make_triples(self, content_item, blender_config, outlinks):
        triples = []
        api = '' 
        post = ['' for i in range(8)]
        from_user_subject = ''
        to_user_subjects = []
        users = []
        from_user = ['' for i in range(7)]
        #flickr
        if  (blender_config['server'], blender_config['interaction']) \
            == ('flickr', 'photos_search'):
            api = 'flickr'
            #post
            post[0] = 'flickr/post/'  + str(content_item['id'])
            post[1] = content_item['id']
            post[2] = 'http://www.flickr.com/%s/%s' % \
                    (content_item['owner'], content_item['id'])
            post[3] = content_item['title']
            #user
            from_user[0] = 'flickr/user/' + str(content_item['owner'])
            from_user_subject = from_user[0]
            from_user[1] = content_item['owner']
            from_user[2] = 'http://www.flickr.com/%s' % (from_user[1])
        #twitter
        if  (blender_config['server'], blender_config['interaction']) \
            == ('twitter-search', 'search'):
            api = 'twitter'
            #post
            post[0] = 'twitter/post/'  + str(content_item['id'])
            post[1] = content_item['id']
            post[2] = 'http://twitter.com/%s/status/%s' % \
                    (content_item['from_user'], content_item['id'])
            post[4] = content_item['text']
            post[5] = content_item['iso_language_code']
            post[6] = content_item['created_at']
            post[7] = content_item.get('geo', '')
            #from user
            from_user[0] = 'twitter/user/' + str(content_item['from_user_id'])
            from_user[1] = content_item['from_user_id']
            from_user[2] =  'http://twitter.com/%s/' % \
                            (content_item['from_user'])
            from_user[3] = content_item['from_user_name']
            from_user[4] = content_item['from_user']
            from_user[5] = content_item['profile_image_url']
            #to users
            entities = content_item.get('entities', {})
            for item in entities.get('user_mentions', [{}]):
                if not item:
                    continue
                to_user = ['' for i in range(7)]
                to_user[0] = 'twitter/user/' + str(item['id'])
                to_user_subjects.append(to_user[0]) 
                to_user[1] = item['id']
                to_user[2] = 'http://twitter.com/%s' % (item['screen_name'])
                to_user[3] = item['name']
                to_user[4] = item['screen_name']
                users.append( to_user )
        if  (blender_config['server'], blender_config['interaction']) \
            == ('facebook', 'search'):
            api = 'facebook'
            #post
            post[0] = 'facebook/post/'  + str(content_item['id'])
            post[1] = content_item['id']
            post[2] = 'http://www.facebook.com/%s' % \
                    (content_item['id'])
            if content_item.get('name',''):
                post[3] = content_item['name']
            if content_item.get('message',''):
                post[4] = content_item['message']
            if content_item.get('caption',''):
                post[4] = content_item['caption']
            post[6] = content_item['updated_time']
            #from user
            from_user[0] = 'facebook/user/' + str(content_item['from']['id'])
            from_user[1] = content_item['from']['id']
            from_user[2] = 'http://www.facebook.com/%s' % \
                    (content_item['from']['id'])
            from_user[3] = content_item['from']['name']
            #to users
            if 'likes' in content_item.keys():
                for item in content_item['likes']['data']:
                    to_user = ['' for i in range(7)]
                    to_user[0] = 'facebook/user/' + str(item['id'])
                    to_user_subjects.append(to_user[0]) 
                    to_user[1] = item['id']
                    to_user[2] = 'http://www.facebook.com/%s' % (item['id'])
                    to_user[3] = item['name']
                    users.append(to_user)
            if 'to' in content_item.keys():
                for item in content_item['to']['data']:
                    to_user = ['' for i in range(7)]
                    to_user[0] = 'facebook/user/' + str(item['id'])
                    to_user_subjects.append(to_user[0]) 
                    to_user[1] = item['id']
                    to_user[2] = 'http://www.facebook.com/%s' % (item['id'])
                    to_user[3] = item['name']
                    users.append(to_user)
        if  (blender_config['server'], blender_config['interaction']) \
            == ('google_plus', 'activities_search'):
            api = 'google_plus'
            #post
            post[0] = 'google_plus/post/'  + str(content_item['id'])
            post[1] = content_item['id']
            post[2] = content_item['url']
            post[3] = content_item['title']
            post[4] = content_item['object']['content']
            post[6] = content_item['published']
            #from user
            from_user[0] =  'google_plus/user/' + \
                            str(content_item['actor']['id'])
            from_user[1] = content_item['actor']['id']
            from_user[2] = content_item['actor']['url'] 
            from_user[3] = content_item['actor']['displayName']
            if 'image' in content_item['actor'].keys():
                from_user[5] = content_item['actor']['image']['url']
            #to users
            #Not available at the moment
        if  (blender_config['server'], blender_config['interaction']) \
            == ('youtube', 'search'):
            api = 'youtube'
            #post
            post[1] = content_item['id']['$t'].split('/')[-1]
            post[0] = 'youtube/post/'  + str(post[1])
            post[2] = 'http://www.youtube.com/watch?v=' + str(post[1])
            post[3] = content_item['title']['$t']
            post[4] = content_item['content']['$t']
            post[6] = content_item['published']['$t']
            #from user
            from_user[1] = content_item['author'][0]['name']['$t']
            from_user[0] = 'youtube/user/' + str(from_user[1])
            from_user[2] = 'http://www.youtube.com/' + str(from_user[1])
            from_user[4] = content_item['author'][0]['name']['$t']
            #to users
            #Not available at the moment
        users.append(from_user)
        from_user_subject = from_user[0]
        #Post
        if not post[0]:
            return []
        triples.extend([
                [ post[0], 'api', api ],
                [ post[0], 'type', 'post' ],
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
                [ user[0], 'type', 'user' ],
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
        new_outlinks = set([triple[2] for triple in triples if \
                        str(triple[1]) == 'url'])
        # TODO: harmonize publication_date (raw pub date et harm pub date)
        # TODO: harmonize location (raw location et harm location)
        # TODO: harmonize language (raw location et harm location)
        return triples, new_outlinks

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
