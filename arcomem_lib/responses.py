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
        # Triples and outlinks are processed for each item of the response 
        # Manually finds the content in the response
        try:
            response_content = self.find_response_content(response)
        except TypeError:
            return 0,0
        blender_config = response['blender_config']
        headers = response['headers']
        # From response content to content item (e.g., from a set of tweets
        # to a unique tweet) 
        if type(response_content) is list:
            for content_item in response_content:
                total_triples, total_outlinks = self.add_content_item(  content_item, 
                                        blender_config, 
                                        headers )
        else:
            total_triples, total_outlinks =\
                self.add_content_item(response_content, blender_config, headers)

    def add_content_item(self, content_item, blender_config, headers):
        try:
            str(content_item['id'])
        except Exception:
            logger.error('Processing the output: %s has no ID' %\
                (content_item))
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
        # Looks into the different paths defined in config.py
        response_content = response['loaded_content']
        for path in config.response_content_path.values():
            found = False
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
        # Getting here means that no path worked, thus it raises an error
        logger.error( 'Could not find content for response: %s' %
                      response['blender_config']['request_url'])
        raise TypeError

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
