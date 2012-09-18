import logging
import re

import config

import warcs
import triples
import outlinks

logger = logging.getLogger('apicrawler')

class ResponsesHandler: 
    """ Handles blender responses """
    def __init__(self): 
        self.triples_handler = triples.TripleManager()
        self.warcs_handler = warcs.WARCManager()
        self.outlinks_handler = outlinks.OutlinksManager()

    def add_response(self, response):
        # The whole response goes as a WARC
        self.warcs_handler.add_response(response)
        # Regarding triples and outlinks, we need to process each item of the
        # response (e.g. a tweet)
        content_items = self.get_content_items(response)
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

    def add_content_item(self, content_item, blender_config, headers):
        try:
            str(content_item['id'])
        except Exception:
            try:
                # Ad hoc add on for facebook users
                # Not great, I agree (cf IDEA up above)
                for key in content_item:
                    content_item[key]['id']
                    content_item = content_item[key]
            except Exception:
                logger.error('Processing the output, could not find a right
                             content item: %s' % (content_item))
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

    def find_response_content(self, response):
        """ Manually finds all the content items (e.g. a tweet) in the
        response """
        content = response['loaded_content']
        # Tries the paths defined in config.py
        server, interaction = response['blender_config']['server'], \
                              response['blender_config']['interaction']
        content_path = config.response_content_paths((server, interaction))
        if not content_path:
            content_items = response['loaded_content']
        found = False
        response_content = response['loaded_content']
        for key in path.split('.'):
            try:
                response_content = response_content[key]
                found = True
            except Exception:
                found = False
                break
            # If one works, it returns directly
            if found:
                return response_content
        # Nothing worked, it returns the loaded content
        return response['loaded_content']

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
