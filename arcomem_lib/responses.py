import logging
import re

import config

import warcs
import triples
import outlinks

logger = logging.getLogger('apicrawler')

class ResponsesHandler: 
    """ Handles API responses """
    def __init__(self): 
        self.triples_handler = triples.TripleManager()
        self.warcs_handler = warcs.WARCManager()
        self.outlinks_handler = outlinks.OutlinksManager()

    def add_response(self, response):
        """ Handles a response dispatching it to the different components
        """
        # The whole response goes as a WARC
        self.warcs_handler.add_response(response)
        # Regarding triples and outlinks, we need to process each item of the
        # response (e.g. a tweet)
        content_items = self.get_content_items(response)
        if not content_items:
            return 0,0
        # Else ..
        blender_config = response['blender_config']
        headers = response['headers']
        total_outlinks, total_triples = 0, 0
        for content_item in content_items:
                item_outlinks, item_triples = self.add_content_item(
                                        content_item, 
                                        blender_config, 
                                        headers )
                total_triples += item_triples
                total_outlinks += item_outlinks
        return total_outlinks, total_triples

    def get_content_items(self, response):
        """ Finds all the content items (e.g. a tweet) in the response """
        content = response['loaded_content']
        content_items = None 
        # Tries the paths defined in config.py
        server, interaction = response['blender_config']['server'], \
                              response['blender_config']['interaction']
        content_path = config.response_content_paths[(server, interaction)]
        if not content_path:
            content_items = []
            content_items.append(response['loaded_content'])
            return content_items
        # Else ..
        found = False
        content_items = content
        for key in content_path.split('.'):
            try:
                content_items = content_items[key]
            except Exception as e:
                logger.warning('Could not parse response, check config.py'\
                    '\nResponse content: %s\nPython error: %s' % (content,e))
                return []
        return content_items 

    def add_content_item(self, content_item, blender_config, headers):
        """ Handles a unique content item (e.g. a tweet) """
        try:
            str(content_item['id'])
        except Exception:
            try:
                # Ad hoc add on for facebook users
                # Not great, IDEA: find a way to improve that 
                for key in content_item:
                    content_item[key]['id']
                    content_item = content_item[key]
            except Exception:
                logger.error('Processing the output, could not find an id'
                             'for content item: %s' % (content_item))
                return None
        init_outlinks = set()
        content_item_outlinks = list(self.extract_outlinks( content_item, 
                                                            init_outlinks))
        _clean_outlinks = set(self.clean_outlinks(content_item_outlinks))
        all_outlinks, total_triples = \
            self.triples_handler.add_content_item(   content_item, 
                                                blender_config, 
                                                _clean_outlinks )
        self.outlinks_handler.add_outlinks(all_outlinks)
        return len(all_outlinks), total_triples

    def extract_outlinks(self, _content, outlinks):
        """ Extracts all outlinks in the content """
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
        """ Cleans outlinks that have been extracted """
        # IDEA: this could be improved, there are still bad outlinks after
        # this step. Could also possibly filter some not very interesting outlinks.
        for outlink in outlinks:
            outlink = outlink.replace("&quot;",'')
            outlink = outlink.replace('"','')
            outlink = outlink.replace('\\','')
        return outlinks
