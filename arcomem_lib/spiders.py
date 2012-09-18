import logging
import string
import urlparse
import json
import datetime

import config

logger = logging.getLogger('apicrawler')


class Spider: 
    """ A spider executes a specific crawl strategy """
    def __init__(self, parameters, start_date, end_date):
        self.statistics = {
            'total_responses': 0,
            'total_triples': 0,
            'total_outlinks': 0
        }
        # Constituents
        self.start_date = start_date
        self.end_date = end_date
        self.parameters = parameters
        # Status, starts as waiting
        self.status = 'waiting'
        # Will be defined after it runs
        self.actual_start_date = None
        self.actual_end_date = None
        self.running_time = None
        self.output_warc = None
        # Used to stop the run
        self.stop_now = False 

    def handle_response(self, response, responses_handler):
        if not response['successful_interaction']:
            return
        # Else ..
        total_outlinks, total_triples = \
                responses_handler.add_response(response)
        self.statistics['total_responses'] += 1
        self.statistics['total_triples'] += total_triples
        self.statistics['total_outlinks'] += total_outlinks

 
    def wrapper_run(self, blender, responses_handler):
        # Pre-run
        self.actual_start_date = datetime.datetime.now()
        self.status = 'running'
        # Runs the crawl
        self.run(blender, responses_handler)
        # Post-run 
        self.actual_end_date = datetime.datetime.now()
        self.status = 'finished'
        running_time = self.actual_end_date - self.actual_start_date 
        self.running_time = running_time.total_seconds()

    def run(self, blender, responses_handler):
        """ Handled by subclasses """
        pass

    def __str__(self):
        if self.start_date:
            start_date_str = \
                self.start_date.strftime(config.datetime_format)
        else:
            start_date_str = 'None'
        if self.end_date:
            end_date_str = \
                self.end_date.strftime(config.datetime_format)
        else:
            end_date_str = 'None'
        if self.actual_start_date:
            actual_start_date_str = \
                self.actual_start_date.strftime(config.datetime_format)
        else:
            actual_start_date_str = 'None'
        if self.actual_end_date:
            actual_end_date_str = \
                self.actual_end_date.strftime(config.datetime_format)
        else:
            actual_end_date_str = 'None'
        str_data = {
                "id": id(self),
                "start_date": start_date_str,
                "end_date": end_date_str,
                "actual_start_date": actual_start_date_str,
                "actual_end_date": actual_start_date_str,
                "running time in seconds": self.running_time,
                "statistics": self.statistics
        }
        return json.dumps(str_data, indent=4, sort_keys=True) 
#
# IDEA: parameters verification method for each spider
#

class FacebookUsers(Spider):
    """ Retrieves a set of facebook users """
    def __init__(self, parameters, start_date, end_date):
        """ Parameters must be a set or a list of facebook usernames or ids """
        Spider.__init__(self, parameters, start_date, end_date)
        self.usernames = set(parameters)
    
    def run(self, blender, responses_handler):
        blender.load_server("facebook")
        blender.load_interaction("user")
        for username in self.usernames:
            if self.stop_now:
                break
            # Else ..
            # Sets parameters and executes interaction
            blender.set_url_params({"ids": username})
            response = blender.blend()
            # Stops here if it is not succesful
            success = response['successful_interaction'] 
            if not success:
                break
            # Else ..
            # Handles response
            self.handle_response(response, responses_handler)


class FacebookSearch(Spider):
    def __init__(self, parameters, start_date, end_date):
        Spider.__init__(self, parameters, start_date, end_date)
        self.keywords_str = string.join(parameters,' ')
    
    def run(self, blender, responses_handler):
        blender.load_server("facebook")
        blender.load_interaction("search")
        success = True
        until = None 
        while success and not self.stop_now: 
            # Sets parameters and executes interaction
            blender.set_url_params({"q": self.keywords_str})
            if until:
                blender.set_url_params({"until": until})
            response = blender.blend()
            # Stops here if it is not succesful
            success = response['successful_interaction'] 
            if not success:
                break
            # Else ..
            # Handles response
            self.handle_response(response, responses_handler)
            # Finds next page instruction
            try:
                next_page_str = response['loaded_content']['paging']['next']
            except Exception:
                break
            query_str = urlparse.urlparse(next_page_str).query
            query_dict = None
            try:
                query_dict = urlparse.parse_qs(query_str)
            except Exception as e:
                logger.error('[Facebook]: Error while parsing %s, error: %s' % 
                        (query_str,e))
            until = None
            for item in query_dict:
                if str(item) == 'until':
                    try:
                        until = int(query_dict[item][0])
                    except Exception as e:
                        logger.error('[Facebook]: Wrong response, check the\
                                API is properly configured, error: %s' % e)


class FlickrSearch(Spider):
    def __init__(self, parameters, start_date, end_date):
        Spider.__init__(self, parameters, start_date, end_date)
        self.keywords_str = string.join(parameters,' ')

    def run(self, blender, responses_handler):
        blender.load_server("flickr")
        blender.load_interaction("photos_search")
        success = True
        p = 0
        pages = 1 
        while p < pages and not self.stop_now: 
            # Sets parameters and executes interaction
            p += 1
            blender.set_url_params({"tags": self.keywords_str, "page": p})
            response = blender.blend()
            # Stops here if it was not successful
            if not response['successful_interaction']:
                break
            # Else..
            # Handles response
            self.handle_response(response, responses_handler)
            # Finds number of pages available
            # (we just need to do that once)
            if p == 1:
                try:
                    pages = response['loaded_content']['photos']['pages']
                except KeyError:
                    logger.error('[FlickR]: Wrong response, check the API is\
                    properly configured (is there an auth file?).')
                    break

class GooglePlusSearch(Spider):
    def __init__(self, parameters, start_date, end_date):
        Spider.__init__(self, parameters, start_date, end_date)
        self.keywords_str = string.join(parameters,' ')
    
    def run(self, blender, responses_handler):
        blender.load_server("google_plus")
        blender.load_interaction("activities_search")
        _continue = True
        pageToken = None
        while _continue and not self.stop_now: 
            # Sets parameters and executes interaction
            blender.set_url_params({"query": self.keywords_str})
            if pageToken:
                blender.set_url_params({"pageToken": pageToken})
            response = blender.blend()
            # Stops here if it was not successful
            success = response['successful_interaction']
            if not success:
                break
            # Else ..
            # Handles response
            self.handle_response(response, responses_handler)
            # Finds next page instruction
            try:
                pageToken = response['loaded_content']['nextPageToken']
            except Exception:
                _continue = False


class TwitterSearch(Spider):
    def __init__(self, parameters, start_date, end_date):
        Spider.__init__(self, parameters, start_date, end_date)
        self.keywords_str = string.join(parameters,' ')
    
    def run(self, blender, responses_handler):
        blender.load_server("twitter-search")
        blender.load_interaction("search")
        success = True
        p = 1
        while success and not self.stop_now: 
            # Sets parameters and executes interaction
            blender.set_url_params({"q": self.keywords_str, "page": p})
            response = blender.blend()
            # Stops here if it was not successful
            success = response['successful_interaction']
            if not success:
                break
            # Else ..
            # Handles response
            self.handle_response(response, responses_handler)
            # Increase page number
            p += 1


class YoutubeSearch(Spider):
    def __init__(self, parameters, start_date, end_date):
        Spider.__init__(self, parameters, start_date, end_date)
        self.keywords_str = string.join(parameters,' ')
    
    def run(self, blender, responses_handler):
        blender.load_server("youtube")
        blender.load_interaction("search")
        success = True
        p = 1
        while success and not self.stop_now: 
            # Sets parameters and executes interaction
            blender.set_url_params({"q": self.keywords_str, "start-index":\
                (p-1)*50+1})
            response = blender.blend()
            # Stops here if it was not successful
            success = response['successful_interaction']
            if not response:
                break
            # Else ..
            # Handles response
            self.handle_response(response, responses_handler)
            # Increase page number
            p += 1

#
# IDEA : More classes could be added, e.g., 
# following a specific user, expanding from a keyword, etc. (future work)
#

#
#       Deprecated
#
#class TwitterSearchAndUsers(Spider):
#    """ Deprecated """
#    def __init__(self, parameters, start_date, end_date):
#        Spider.__init__(self, parameters, start_date, end_date)
#
#    def set_keywords(self, keyword):
#        self.keywords = keywords
#    
#    def run(self, blender, responses_handler):
#        blender.load_server("twitter-search")
#        blender.load_interaction("search")
#        users = set()
#        p = 0
#        success = True
#        while success: 
#            p += 1
#            blender.set_url_params({"q": self.keyword, "page": p})
#            response = blender.blend()
#            if not response:
#                break
#            for twitt in response['loaded_content']['results']:
#                users.add(twitt['from_user'])
#        blender.load_server("twitter-generic")
#        for user in users:
#            logger.info("User Name: %s" % user)
#            blender.load_interaction('followers')
#            blender.set_url_params({"screen_name": user})
#            response = blender.blend()
#            logger.info("\tFollowers: %s" % response['loaded_content'])
#            blender.load_interaction('followees')
#            blender.set_url_params({"screen_name": user})
#            response = blender.blend()
#            logger.info("\tFollowees: %s" % response['loaded_content'])
