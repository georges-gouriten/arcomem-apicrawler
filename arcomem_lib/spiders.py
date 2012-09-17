import logging
import string
import urlparse

logger = logging.getLogger('apicrawler')

class Spider: 
    """ A spider executes a specific crawl strategy """
    def __init__(self, parameters):
        self.statistics = {
            'total_responses': 0,
            'total_triples': 0,
            'total_outlinks': 0
        }
        self.parameters = parameters
        self.stop_now = False 

    def handle_response(self, response, responses_handler):
        if not response['successful_interaction']:
            return
        # Else ..
        total_outlinks, total_triples = responses_handler.add_response(response)
        self.statistics['total_responses'] += 1
        self.statistics['total_triples'] += total_triples
        self.statistics['total_outlinks'] += total_outlinks

    def order_stop(self):
        self.stop_now = True

 
# 
# IDEA: The class definitions made here might be improved
#

class FacebookSearch(Spider):
    def __init__(self, parameters):
        Spider.__init__(self, parameters)
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
    def __init__(self, parameters):
        Spider.__init__(self, parameters)
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
    def __init__(self, parameters):
        Spider.__init__(self, parameters)
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
    def __init__(self, parameters):
        Spider.__init__(self, parameters)
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
    def __init__(self, parameters):
        Spider.__init__(self, parameters)
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
# Deprecated
#
class DeprecatedTwitterSearchAndUsers(Spider):
    """ Deprecated """
    def __init__(self, parameters):
        Spider.__init__(self, parameters)

    def set_keywords(self, keyword):
        self.keywords = keywords
    
    def run(self, blender, responses_handler):
        blender.load_server("twitter-search")
        blender.load_interaction("search")
        users = set()
        p = 0
        success = True
        while success: 
            p += 1
            blender.set_url_params({"q": self.keyword, "page": p})
            response = blender.blend()
            if not response:
                break
            for twitt in response['loaded_content']['results']:
                users.add(twitt['from_user'])
        blender.load_server("twitter-generic")
        for user in users:
            logger.info("User Name: %s" % user)
            blender.load_interaction('followers')
            blender.set_url_params({"screen_name": user})
            response = blender.blend()
            logger.info("\tFollowers: %s" % response['loaded_content'])
            blender.load_interaction('followees')
            blender.set_url_params({"screen_name": user})
            response = blender.blend()
            logger.info("\tFollowees: %s" % response['loaded_content'])
