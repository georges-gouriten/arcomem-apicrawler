import logging
import Queue
from threading import Thread
import datetime
import os
import json
import time

import config

logger = logging.getLogger('triples')


class TripleManager:
    """ Makes and handles triples """
    def __init__(self):
        self.triples_queue = Queue.Queue()
        self.start_daemon()
        logger.info('Triples Manager started')
        self.set_new_file()
        self.chunk_counter = 0

    def start_daemon(self):
        triples_daemon_thread = Thread(target=self.triples_daemon)
        triples_daemon_thread.start() 

    def triples_daemon(self): 
        """ Looks into the triples queue and sends chunks to the triple
        store or saves it into the backup file if it failed """
        while True:
            chunk = []
            chunk_size=config.triples_chunk_size
            save_backup = False
            while len(chunk) < chunk_size:
                # Waits for the chunk to be full
                chunk.append(self.triples_queue.get(True))
            logger.info('[In progress] Sending %s triples to the triple store' 
                        % chunk_size)
            try:
                # TODO: use put_triples method provided by Nikos
                raise NotImplementedError, 'waiting for Nikos' 
                logger.info('[Success] Sent %s triples to the triple store' 
                            % chunk_size)
            except Exception as e:
                logger.warning('[Failure] Exception occured during an '
                               'attempt to send the triples: %s' % e)
                save_backup = True
            # Writes non transferred triples in a backup file
            if save_backup:
                #
                # IDEA: there could be a retry mecanism for non transferred
                # triples
                #
                # Creates a new backup file if the current one is too big
                try:
                    size = os.path.getsize(self.current_file)
                except OSError:
                    size = 0
                if size > 500 * 1024 * 1024:
                    self.set_new_file()
                logger.info('[In progress] Saving triples to backup file' )
                with open(self.current_file, 'a') as _f:
                    _f.write( ' ** Backup **\nDate: ' + \
                    datetime.datetime.now().strftime(config.datetime_format)\
                    + '\n')
                    for triple in chunk:
                        _f.write(json.dumps(triple) + '\n')
                logger.info('[Success] Saved triples to backup file')

    def set_new_file(self):
        """ Changes the backup file """
        file_name = \
            datetime.datetime.now().strftime(config.datetime_format) \
            + '.txt'
        self.current_file = os.path.join(config.triples_path, file_name)
        logger.info('Backup file: %s' 
                     % self.current_file) 
   
    def add_content_item(self, content_item, blender_config, outlinks):
        """ Makes triples from a content_item and adds them to the queue """
        triples = []
        new_outlinks = set()
        try:
            triples, new_outlinks = \
                    self.make_triples(content_item, blender_config, outlinks)
        except Exception as e:
            logger.error('Could not convert %s, error: %s'
                         % (content_item,e))
        for triple in triples:
            self.triples_queue.put(triple)
        return outlinks.union(new_outlinks), self.triples_queue.qsize()
        

    #
    # IDEAs: 
    # This part could be arranged with separated methods
    # triples could be marked with a datetime
    # (has to be discussed with the triple consumers)
    # harmonize publication_date (raw pub date et harm pub date)
    # harmonize location (raw location et harm location)
    # harmonize language (raw location et harm location)
    #
    def make_triples(self, content_item, blender_config, outlinks):
        """ Makes harmonized triples """
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
            == ('facebook', 'users'):
                user = ['' for i in range(7)]
                user[0] = 'facebook/user/'  + str(content_item['id'])
                user[1] = content_item['id']
                user[2] = 'http://www.facebook.com/%s' % (content_item['id'])
                user[3] = content_item['name']
                user[4] = content_item['username']
                if content_item.get('location',''):
                    user[6] = content_item['location']
                users.append(user)
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
        return triples, new_outlinks

#
#       Deprecated socket communication
#
#    def initiate_socket_connection(self, host='localhost', port=19898):
#        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#        self.s.connect((host, port))
#
#    def close_socket(self):
#        self.s.close()

#
#               Deprecated triple store socket communication
#
#               self.initiate_socket_connection()
#               self.s.send(string_chunk)
#               self.close_socket()
#

#
#       Deprecated method used for the socket communication
##
#    def stringify_triples(self, triples):
#        string_triples = []
#        for triple in triples:
#            string_triple = []
#            for position, item  in enumerate(triple):
#                if position < 2:
#                    item = 'apicrawler:' + json.dumps(item)
#                else:
#                    item = json.dumps(item)
#                string_triple.append(item.replace('"',''))
#            string_triple = "\\SPO".join(string_triple)
#            string_triples.append(string_triple)
#        string_triples = "\\EOT".join(string_triples)
#        string_triples += "\\EOL"
#        return string_triples
