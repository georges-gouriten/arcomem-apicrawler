from datetime import datetime
import utils
import Queue
from threading import Thread
import string
import urlparse
import time
import weakref
import logging

import output
import apiblender

""" 
This module contains the different Spiders
"""

PLATFORMS = [ 'facebook', 'flickr', 'google_plus', 'twitter', 'youtube' ]

class SpidersController:

    def __init__(self):
        self.responses_handler = output.ResponsesHandler()
        self.campaigns = []
        self.platforms = []
        self.crawls = []
        for PLATFORM in PLATFORMS:
            platform = Platform(PLATFORM, self.responses_handler)
            self.platforms.append(platform)
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

    def __init__(self, name, responses_handler):
        self.name = name
        self.queue = Queue.Queue()
        self.thread = Thread(target=self.platform_daemon)
        self.blender =  apiblender.Blender()
        self.responses_handler = responses_handler
        self.thread.start()

    def platform_daemon(self):
        while True:
            crawl = self.queue.get()
            crawl.run(self.blender, self.responses_handler)

    def add_crawl_to_queue(self, crawl):
        self.queue.put(crawl)


class Campaign:

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
    
    # This part could be reshaped
    def __init__(self):
        self.stats = []
        for i in range(0, len(PLATFORMS)):
            row = []
            for j in range(0, 4):
                row.append(0)
            self.stats.append(row)

    def change_status(self, status_before, status_now, platform_name):
        i = PLATFORMS.index(platform_name)
        if status_before > -1:
            self.stats[i][status_before] -= 1
        self.stats[i][status_now] += 1

    def increase_requests(self, num_req, platform_name):
        i = PLATFORMS.index(platform_name)
        self.stats[i][3] += num_req

    def str(self):
        i = 0
        _string = ''
        for PLATFORM in PLATFORMS:
            _string += "---" + "\n{0:20}".format(PLATFORM) + \
                "\n\t{0:20} {1}".format('Waiting:', stats[i][0])+\
                "\n\t{0:20} {1}".format('Running:', stats[i][1])+\
                "\n\t{0:20} {1}\n".format('Finished:', stats[i][2])
            i += 1
        return _string

class Crawl:

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
        self.status = 1
        self.campaign.statistics.change_status(0, 1, self.platform.name) 
        # Strategy is not used at the moment (only search strategy)
        keywords = self.parameters
        if str(self.platform.name) == 'twitter':
            new_spider = TwitterPages(responses_handler)
            new_spider.set_keywords(keywords)
        elif str(self.platform.name) == 'facebook':
            new_spider = FacebookPages(responses_handler)
            new_spider.set_keywords(keywords)
        elif str(self.platform.name) == 'google_plus':
            new_spider = GoogleplusPages(responses_handler)
            new_spider.set_keywords(keywords)
        elif str(self.platform.name) == 'flickr':
            new_spider = FlickrPages(responses_handler)
            new_spider.set_keywords(keywords)
        elif str(self.platform.name) == 'youtube':
            new_spider = YoutubePages(responses_handler)
            new_spider.set_keywords(keywords)
        else:
            #TODO
            return
        new_spider.run(blender)
        self.end_date = datetime.now()
        self.status = 2
        self.requests_count = new_spider.requests_count
        self.campaign.statistics.increase_requests(self.requests_count,\
                self.platform.name)
        self.campaign.statistics.change_status(1, 2, self.platform.name) 

    def set_id(self, _id):
        self._id = _id

    def str(self):
        if self.status == 2:
            end_date = self.end_date
        elif self.status == 1:
            end_date = 'Still running'
        elif self.status == 0:
            end_date = 'Waiting'
        return  "\n\t---" +\
                "\n\t{0:20} {1}".format('Start date:', self.start_date) +\
                "\n\t{0:20} {1}".format('Platform:', self.platform.name) +\
                "\n\t{0:20} {1}".format('Keywords:', self.keywords) +\
                "\n\t{0:20} {1}".format('End date:', end_date)


class Spider: 

    def __init__(self, responses_handler):
        self.beginning_date = datetime.now()
        self.requests_count = 0
        self.responses_handler = responses_handler

    def handle_response(self, response):
        response.update({ "triple_prefix": self.TRIPLE_PREFIX })
        self.responses_handler.add_response(response)
 

class TwitterPages(Spider):

    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)
        self.TRIPLE_PREFIX = "twitter/post/"

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
        blender.load_server("twitter-search")
        blender.load_interaction("search")
        success = True
        p = 0
        while success: 
            p += 1
            blender.set_url_params({"q": self.keywords_str, "page": p})
            response = blender.blend()
            if not response:
                break
            self.requests_count += 1
            success = ( 300 > response["headers"]['status'] >= 200 )
            if success:
                self.handle_response(response)


class FacebookPages(Spider):

    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)
        self.TRIPLE_PREFIX = "facebook/post/"

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
        blender.load_server("facebook")
        blender.load_interaction("search")
        success = True
        until = None 
        while success: 
            # TODO: error handling, keyword
            blender.set_url_params({"q": self.keywords_str})
            if until:
                blender.set_url_params({"until": until})
            response = blender.blend()
            if not response:
                break
            self.requests_count += 1
            try:
                next_page_str = response['prepared_content']['paging']['next']
            except Exception:
                break
            query_str = urlparse.urlparse(next_page_str).query
            query_dict = None
            try:
                query_dict = urlparse.parse_qs(query_str)
            except Exception as e:
                logging.error('URL parsing: %s, error: %s' % (query_str,e))
            until = None
            for item in query_dict:
                if str(item) == 'until':
                    try:
                        until = int(query_dict[item][0])
                    except Exception as e:
                        logging.error('Facebook until field: %s, error: %s' % \
                                (until, e))

            success = 300 > response["headers"]['status'] >= 200 and until
            if success:
                self.handle_response(response)


class GoogleplusPages(Spider):

    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)
        self.TRIPLE_PREFIX = "google_plus/post/"

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
        blender.load_server("google_plus")
        blender.load_interaction("activities_search")
        _continue = True
        pageToken = None
        while _continue: 
            # TODO: error handling, keyword
            blender.set_url_params({"query": self.keywords_str})
            if pageToken:
                blender.set_url_params({"pageToken": pageToken})
            response = blender.blend()
            if not response:
                break
            try:
                pageToken = response['prepared_content']['nextPageToken']
            except Exception:
                _continue = False
            self.requests_count += 1
            success = ( 300 > response["headers"]['status'] >= 200 )
            if success:
                self.handle_response(response)
            else:
                _continue = False


class YoutubePages(Spider):

    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)
        self.TRIPLE_PREFIX = "youtube/post/"

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
        blender.load_server("youtube")
        blender.load_interaction("search")
        success = True
        p = 0
        while success: 
            p += 1
            # TODO: error handling, keyword
            blender.set_url_params({"q": self.keywords_str, "start-index":\
                (p-1)*50+1})
            response = blender.blend()
            if not response:
                break
            self.requests_count += 1
            success = ( 300 > response["headers"]['status'] >= 200 )
            if success:
                self.handle_response(response)


class FlickrPages(Spider):

    def __init__(self, responses_handler):
        Spider.__init__(self, responses_handler)
        self.TRIPLE_PREFIX = "flickr/post/"

    def set_keywords(self, keywords):
        self.keywords_str = string.join(keywords,' ')
    
    def run(self, blender):
        blender.load_server("flickr")
        blender.load_interaction("photos_search")
        success = True
        p = 0
        pages = 1 
        while p < pages: 
            p += 1
            # TODO: error handling, keyword
            blender.set_url_params({"tags": self.keywords_str, "page": p})
            response = blender.blend()
            if not response:
                break
            # Manual definition of maximum page
            if p == 1:
                pages = response['prepared_content']['photos']['pages']
            self.requests_count += 1
            success = ( 300 > response["headers"]['status'] >= 200 )
            if success:
                self.handle_response(response)


class TwitterPagesAndUsers(Spider):

    def __init__(self, responses_handler):
        Spider.__init__(self, results_handler)
        self.TRIPLE_PREFIX = "twitter/post/"

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
            for twitt in response['prepared_content']['results']:
                users.add(twitt['from_user'])
        blender.load_server("twitter-generic")
        for user in users:
            logging.info("User Name: %s" % user)
            blender.load_interaction('followers')
            blender.set_url_params({"screen_name": user})
            response = blender.blend()
            logging.info("\tFollowers: %s" % response['prepared_content'])
            blender.load_interaction('followees')
            blender.set_url_params({"screen_name": user})
            response = blender.blend()
            logging.info("\tFollowees: %s" % response['prepared_content'])
