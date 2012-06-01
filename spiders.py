from datetime import datetime
import utils
import Queue
from threading import Thread
import string

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

    def new_crawl(self, campaign_name, platform_name, keywords):
        campaign = self.get_campaign(campaign_name)
        if not campaign:
            campaign = Campaign(campaign_name)
            self.campaigns.append(campaign)
        platform = self.get_platform(platform_name)
        crawl = Crawl(campaign, platform, keywords)
        platform.add_crawl_to_queue(crawl)
        self.crawls.append(crawl)

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
    
    def __init__(self):
        self.stats = []
        for i in range(0, len(PLATFORMS)):
            row = []
            for j in range(0,3):
                row.append(0)
            self.stats.append(row)


    def change_status(self, status_before, status_now, platform_name):
        print self.stats
        print status_now
        i = PLATFORMS.index(platform_name)
        print i
        if status_before > -1:
            self.stats[i][status_before] -= 1
        self.stats[i][status_now] += 1

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

    def __init__(self, campaign, platform, keywords):
        self.platform = platform
        self.campaign = campaign
        self.keywords = keywords
        self.start_date = datetime.now()
        # Statut: 0 is waiting, 1 is running, 2 is finished
        self.status = 0
        self.campaign.statistics.change_status(-1, 0, self.platform.name) 
        self.end_date = False

    def run(self, blender, responses_handler):
        self.status = 1
        self.campaign.statistics.change_status(0, 1, self.platform.name) 
        if str(self.platform.name) == 'twitter':
            new_spider = TwitterPages(responses_handler)
            new_spider.set_keywords(self.keywords)
        if str(self.platform.name) == 'facebook':
            new_spider = FacebookPages(responses_handler)
            new_spider.set_keywords(self.keywords)
        if str(self.platform.name) == 'google_plus':
            new_spider = GoogleplusPages(responses_handler)
            new_spider.set_keywords(self.keywords)
        if str(self.platform.name) == 'flickr':
            new_spider = FlickrPages(responses_handler)
            new_spider.set_keywords(self.keywords)
        if str(self.platform.name) == 'youtube':
            new_spider = YoutubePages(responses_handler)
            new_spider.set_keywords(self.keywords)
        else:
            #TODO
            return
        new_spider.run(blender)
        self.end_date = datetime.now()
        self.status = 2
        self.campaign.statistics.change_status(1, 2, self.platform.name) 

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
        self.request_count = 0
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
            # TODO: error handling, keyword
            blender.set_url_params({"q": self.keywords_str, "page": p})
            response = blender.blend()
            self.request_count += 1
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
        p = 0
        while success: 
            p += 1
            # TODO: error handling, keyword
            blender.set_url_params({"q": self.keywords_str, "page": p})
            response = blender.blend()
            self.request_count += 1
            success = ( 300 > response["headers"]['status'] >= 200 )
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
            try:
                pageToken = response['prepared_content']['nextPageToken']
            except Exception:
                _continue = False
            self.request_count += 1
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
            self.request_count += 1
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
            # Manual definition of maximum page
            if p == 1:
                pages = response['prepared_content']['photos']['pages']
            self.request_count += 1
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
            for twitt in response['prepared_content']['results']:
                users.add(twitt['from_user'])
        blender.load_server("twitter-generic")
        for user in users:
            print("User Name: %s" % user)
            blender.load_interaction('followers')
            blender.set_url_params({"screen_name": user})
            response = blender.blend()
            print("\tFollowers: %s" % response['prepared_content'])
            blender.load_interaction('followees')
            blender.set_url_params({"screen_name": user})
            response = blender.blend()
            print("\tFollowees: %s" % response['prepared_content'])
            

