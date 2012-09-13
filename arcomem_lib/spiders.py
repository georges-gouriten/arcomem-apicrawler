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
        self.crawls = []
        for platform_str in config.platforms:
            platform = Platform(platform_str, self.responses_handler)
            self.platforms.append(platform)
        # Stores permanently some objects: the crawls
        self._id2obj_dict = weakref.WeakValueDictionary()

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

    def get_campaign(self, campaign_name):
        campaign = False
        for _campaign in self.campaigns:
            if str(_campaign.name) == str(campaign_name):
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
        [campaign_name, platform_name, strategy, parameters] = crawl
        campaign = self.get_campaign(campaign_name)
        if not campaign:
            campaign = Campaign(campaign_name)
            self.campaigns.append(campaign)
        platform = self.get_platform(platform_name)
        crawl = Crawl(campaign, platform, strategy, parameters)
        crawl_id = self.remember_object(crawl)
        crawl.set_id(crawl_id)
        platform.add_crawl_to_queue(crawl)
        self.crawls.append(crawl)
        return crawl_id

    def del_campaign(self, campaign_name):
        campaign = self.get_campaign(campaign_name)
        for i, _campaign in enumerate(self.campaigns):
            if str(campaign.name) == str(_campaign.name):
                del self.campaigns[i]
        del _campaign
        del campaign


    def get_campaign_names(self):
        return [campaign.name for campaign in self.campaigns]

    def get_crawls(self, campaign_name):
        crawls = []
        for crawl in self.crawls:
            if str(crawl.campaign.name) == str(campaign_name):
                crawls.append(crawl)
        return crawls

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
    def __init__(self, name):
        self.name = name
        self.start_date = datetime.now()
        self.statistics = CampaignStatistics()

    def str(self):
        return  "{0:20} {1}".format('Id:', self.name) +\
                "\n\t---" +\
                "\n\t{0:20} {1}".format('Start date:', self.start_date)


STATUSES = { -1: 'tmp', 0: 'waiting', 1: 'running', 2: 'finished' }


class CampaignStatistics:
    """ Statistics belonging to a campaign """
    # TODO: This part could be reshaped, the stats array is not that
    # convenient.
    def __init__(self):
        self.stats = []
        for i in range(0, len(config.platforms)):
            row = []
            for j in range(0, 4):
                row.append(0)
            self.stats.append(row)

    def change_status(self, status_before, status_now, platform_name):
        i = config.platforms.index(platform_name)
        if status_before > -1:
            self.stats[i][status_before] -= 1
        self.stats[i][status_now] += 1

    def increase_requests(self, num_req, platform_name):
        i = config.platforms.index(platform_name)
        self.stats[i][3] += num_req

    def str(self):
        i = 0
        _string = ''
        for platform_str in config.platforms:
            _string += "---" + "\n{0:20}".format(_platform) + \
                "\n\t{0:20} {1}".format('Waiting:', stats[i][0])+\
                "\n\t{0:20} {1}".format('Running:', stats[i][1])+\
                "\n\t{0:20} {1}\n".format('Finished:', stats[i][2])
            i += 1
        return _string

class Crawl:
    """ A crawl is a set of request corresponding to a specific strategy """
    def __init__(self, campaign, platform, strategy, parameters):
        self.platform = platform
        self.campaign = campaign
        self.strategy = strategy
        self.parameters = parameters
        self.start_date = datetime.now()
        # Status: 0 is waiting, 1 is running, 2 is finished
        self.status = 0
        self.campaign.statistics.change_status(-1, 0, self.platform.name) 
        self.end_date = False
        self.requests_count = 0
        self._id = False

    def run(self, blender, responses_handler):
        self.platform.logger.info('[Starting crawl] %s', self.str())
        self.status = 1
        self.campaign.statistics.change_status(0, 1, self.platform.name) 
        # Strategy is not used at the moment (only search strategy)
        keywords = self.parameters
        # Redirects to the right spider
        # IDEA: improve this
        if str(self.platform.name) == 'twitter':
            new_spider = TwitterSearch(responses_handler)
            new_spider.set_keywords(keywords)
        elif str(self.platform.name) == 'facebook':
            new_spider = FacebookSearch(responses_handler)
            new_spider.set_keywords(keywords)
        elif str(self.platform.name) == 'google_plus':
            new_spider = GoogleplusSearch(responses_handler)
            new_spider.set_keywords(keywords)
        elif str(self.platform.name) == 'flickr':
            new_spider = FlickrSearch(responses_handler)
            new_spider.set_keywords(keywords)
        elif str(self.platform.name) == 'youtube':
            new_spider = YoutubeSearch(responses_handler)
            new_spider.set_keywords(keywords)
        else:
            #TODO
            return
        # The spider takes care of running the crawl
        new_spider.run(blender)
        # Once it is done, we update some statistics
        self.end_date = datetime.now()
        self.status = 2
        self.requests_count = new_spider.requests_count
        self.campaign.statistics.increase_requests(self.requests_count,\
                self.platform.name)
        self.campaign.statistics.change_status(1, 2, self.platform.name) 
        self.platform.logger.info('[Completed crawl] %s' % self.str())
        # Checks the crawl duration
        crawl_duration = self.end_date - self.start_date
        crawl_duration_s = crawl_duration.total_seconds()
        if (crawl_duration_s < 5):
            logger.warning( 'Crawl duration < 5s for %s' %
                            self.str())

    def set_id(self, _id):
        self._id = _id

    def str(self):
        if self.status == 2:
            end_date = self.end_date
        elif self.status == 1:
            end_date = 'still running'
        elif self.status == 0:
            end_date = 'waiting'
        return  "id: %s - start date: %s - platform: %s - parameters: %s" \
                " - end date: %s" % (self._id, self.start_date,
                                self.platform.name, self.parameters, end_date)


class Spider: 
    """ A spider executes a crawl """
    def __init__(self, responses_handler):
        self.beginning_date = datetime.now()
        self.requests_count = 0
        self.responses_handler = responses_handler

    def handle_response(self, response):
        self.responses_handler.add_response(response)
 
# 
# IDEA: The class definitions made here might be improved
#

class FacebookSearch(Spider):
    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
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
            self.requests_count += 1
            # Stops here if it is not succesful
            success = response['successful_interaction'] 
            if not success:
                break
            # Else ..
            # Handles response
            self.handle_response(response)
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
    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
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
            self.requests_count += 1
            # Stops here if it was not successful
            if not response['successful_interaction']:
                break
            # Else..
            # Finds the number of pages available
            # (we just need to do that once)
            if p == 1:
                try:
                    pages = response['loaded_content']['photos']['pages']
                except KeyError:
                    logger.error('[FlickR]: Wrong response, check the API is\
                    properly configured (is there an auth file?).')
                    break

class GoogleplusSearch(Spider):
    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
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
            self.requests_count += 1
            # Stops here if it was not successful
            success = response['successful_interaction']
            if not success:
                break
            # Else ..
            # Handles response
            self.handle_response(response)
            # Finds next page instruction
            try:
                pageToken = response['loaded_content']['nextPageToken']
            except Exception:
                _continue = False


class TwitterSearch(Spider):
    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
        blender.load_server("twitter-search")
        blender.load_interaction("search")
        success = True
        p = 1
        while success: 
            # Sets parameters and executes interaction
            blender.set_url_params({"q": self.keywords_str, "page": p})
            response = blender.blend()
            self.requests_count += 1
            # Stops here if it was not successful
            success = response['successful_interaction']
            if not success:
                break
            # Else ..
            # Handles response
            self.handle_response(response)
            # Increase page number
            p += 1


class YoutubeSearch(Spider):
    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
        blender.load_server("youtube")
        blender.load_interaction("search")
        success = True
        p = 1
        while success: 
            # Sets parameters and executes interaction
            blender.set_url_params({"q": self.keywords_str, "start-index":\
                (p-1)*50+1})
            response = blender.blend()
            self.requests_count += 1
            # Stops here if it was not successful
            success = response['successful_interaction']
            if not response:
                break
            # Else ..
            # Handles response
            self.handle_response(response)
            # Increase page number
            p += 1

#
# Deprecated
#
class DeprecatedTwitterSearchAndUsers(Spider):
    """ Deprecated """
    def __init__(self, responses_handler):
        Spider.__init__(self, results_handler)

    def set_keywords(self, keyword):
        self.keywords = keywords
    
    def run(self, blender):
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
