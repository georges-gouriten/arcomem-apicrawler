from datetime import datetime
import Queue
from threading import Thread
import string
import urlparse
import time
import weakref
import logging

import apiblender

import config
import utils
import responses

""" 
This module describes the different Spiders
"""

logger = logging.getLogger('apicrawler')

class SpidersController:
    """ Controls the spiders execution """
    def __init__(self):
        self.responses_handler = responses.ResponsesHandler()
        self.campaigns = []
        self.platforms = []
        for platform_str in config.platforms:
            platform = Platform(platform_str, self.responses_handler)
            self.platforms.append(platform)

    def get_campaign(self, campaign_id):
        campaign = False
        for _campaign in self.campaigns:
            if str(_campaign._id) == str(campaign_id):
                campaign = _campaign 
                break
        return campaign

    def get_platform(self, platform_name):
        platform = False
        for _platform in self.platforms:
            if str(_platform.name) == str(platform_name):
                platform = _platform 
                break
        return platform

    def add_crawl(self, crawl):
        [campaign_id, platform_name, strategy, parameters] = crawl
        # Gets the right campaign and platform
        campaign = self.get_campaign(campaign_id)
        if not campaign:
            campaign = Campaign(campaign_id)
            self.campaigns.append(campaign)
        platform = self.get_platform(platform_name)
        # The campaign object takes care of creating the crawl
        crawl_id = campaign.add_crawl(platform, strategy, parameters)
        return crawl_id

    def del_campaign(self, campaign_id):
        campaign = self.get_campaign(campaign_id)
        for i, _campaign in enumerate(self.campaigns):
            if str(campaign._id) == str(_campaign._id):
                del self.campaigns[i]
        del _campaign
        del campaign

    def get_campaign_ids(self):
        return [campaign._id for campaign in self.campaigns]

    def get_crawls(self, campaign_id):
        campaign = self.get_campaign(campaign_id)
        return campaign.crawls

    def get_load(self):
        return [platform.queue.qsize() for platform in self.platforms]


class Platform:
    """ A platform corresponds to a service, e.g., a facebook platform for
    the facebook API service """
    def __init__(self, name, responses_handler):
        self.name = name
        self.queue = Queue.Queue()
        self.thread = Thread(target=self.platform_daemon)
        self.blender =  apiblender.Blender()
        self.responses_handler = responses_handler
        self.thread.start()
        self.logger = logging.getLogger(self.name)

    def platform_daemon(self):
        while True:
            crawl = self.queue.get()
            crawl.run(self.blender, self.responses_handler)

    def add_crawl_to_queue(self, crawl):
        self.queue.put(crawl)


class Campaign:
    """ A campaign is a set of crawls """
    def __init__(self, _id):
        self._id = _id
        self.start_date = datetime.now()
        self.statistics = CampaignStatistics()
        # Stores permanently some objects: the crawls
        self.crawls = []
        self._id2obj_dict = weakref.WeakValueDictionary()

    def str(self):
        return  "id: %s - start date: %s" % (self._id, self.start_date)
    
    def add_crawl(self, platform, strategy, parameters):
        crawl = Crawl(self, platform, strategy, parameters)
        crawl_id = self.remember_object(crawl)
        crawl.set_id(crawl_id)
        platform.add_crawl_to_queue(crawl)
        self.crawls.append(crawl)
        return crawl_id

    def remember_object(self, obj):
        oid = id(obj)
        self._id2obj_dict[oid] = obj
        return oid

    def object_from_id(self, oid):
        _object = None
        try: 
            oid = int(oid)
            _object = self._id2obj_dict[oid]
        except Exception:
            pass
        return _object


class CampaignStatistics:
    """ Statistics belonging to a campaign """
    def __init__(self):
        self.stat_dict = {}
        for platform in platforms:
            self.stat_dict.update(
                {
                    platform: {
                        'total_finished_crawls': 0,
                        'total_responses': 0,
                        'total_triples': 0
                        'total_outlinks': 0
                    }
                }   
            )

    def add_crawl_statistics(self, statistics, platform_name):
        total_responses = statistics['total_responses']
        total_outlinks = statistics['total_outlinks']
        total_triples = statistics['total_triples']
        self.stat_dict[platform_name]['total_finished_crawls'] += 1
        self.stat_dict[platform_name]['total_responses'] += total_responses
        self.stat_dict[platform_name]['total_triples'] += total_triples
        self.stat_dict[platform_name]['total_outlinks'] += total_outlinks 

class Crawl:
    """ A crawl is a set of request corresponding to a specific strategy """
    def __init__(self, campaign, platform, strategy, parameters):
        # Id is set externally
        self._id = None
        # Constituents
        self.platform = platform
        self.campaign = campaign
        self.strategy = strategy
        self.parameters = parameters
        # Time considerations
        self.start_date = None 
        self.end_date = None
        self.duration = None
        # Statistics
        self.statistics = None
        # Status
        self.set_status(0)
        # Creates the right spider
        spider_class = config.spider_mapping[(platform.name, strategy)]
        self.spider = eval(spider_class + '(parameters)')

    def set_status(status):
        """ TODO: define the right statuses """
        self.status = status

    def run(self, blender, responses_handler):
        # Pre-run
        self.start_date = datetime.now()
        self.platform.logger.info('[Starting crawl] %s', self.str())
        self.set_status(1)
        # Runs the crawl
        self.spider.run(blender, responses_handler)
        # Post-run 
        self.end_date = datetime.now()
        self.platform.logger.info('[Completed crawl] %s' % self.str())
        self.set_status(2)
        self.statistics = self.spider.statistics
        self.campaign.statistics.add_crawl_statistics(self.statistics,\
                self.platform.name)
        # Checks the crawl duration
        crawl_duration = self.end_date - self.start_date
        self.duration = crawl_duration.total_seconds()
        if (self.duration < 5):
            logger.warning( 'Crawl duration < 5 seconds for %s' % self.str())

    def set_id(self, _id):
        self._id = _id

    def str(self):
        return  "id: %s - duration (s): %s - platform: %s - strategy: %s -"\
                " parameters: %s - # requests: %s" % (self._id,
                        self.duration, self.platform.name, self.strategy,
                        self.parameters, self.requests_count)


class Spider: 
    """ A spider executes a specific crawl strategy """
    def __init__(self, parameters):
        self.statistics = {
            'total_responses': 0,
            'total_triples': 0,
            'total_outlinks': 0
        }
        self.parameters = parameters

    def handle_response(self, response, responses_handler):
        if not response['successful_interaction']:
            return
        # Else ..
        total_triples, total_outlinks = self.responses_handler.add_response(response)
        self.statistics['total_responses'] += 1
        self.statistics['total_triples'] += total_triples
        self.statistics['total_outlinks'] += total_outlinks

 
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
        while success: 
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
        while p < pages: 
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

class GoogleplusSearch(Spider):
    def __init__(self, parameters):
        Spider.__init__(self, parameters)
        self.keywords_str = string.join(parameters,' ')
    
    def run(self, blender, responses_handler):
        blender.load_server("google_plus")
        blender.load_interaction("activities_search")
        _continue = True
        pageToken = None
        while _continue: 
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
        while success: 
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
        while success: 
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
