import datetime
import Queue
from threading import Thread
import string
import urlparse
import time
import logging
import math

import apiblender

import config
import utils
import responses

""" 
This module describes the different Spiders
"""

logger = logging.getLogger('apicrawler')

class APICrawlerInterface:
    """ Interface to the external world """
    def __init__(self):
        logger.info('Starting the APICrawler')
        self.responses_handler = responses.ResponsesHandler()
        # Contains the crawls
        self.crawls = set()
        # Contains the campaign ids 
        self.campaign_ids = set() 
        # Contains the platform objects
        self.platforms = set()
        # Initializes platforms
        for platform_str in config.platforms:
            logger.info('Starting platform %s' % platform_str)
            platform = Platform(platform_str, self.responses_handler)
            self.platforms.add(platform)
        logger.info('APICrawler is ready')

    #
    #   Methods used by the pipeline
    #

    def add_crawl(  self, 
                    platform_name,          # String, e.g. twitter
                    strategy,               # String, e.g. search 
                    parameters,             # Anything 
                    campaign_id,            # String
                    crawl_id=None,          # If already defined 
                    start_date=None, 
                    end_date=None, 
                    period_in_hours=0       # If > 0, repeats every x hours
                ):
        """ Generic method to add a crawl to the queue, if a period and an
        end date is specified, it can add several crawlers. """
        # Gets the right platform (see below)
        platform = self.get_platform(platform_name)
        # Quick trick about the start date
        if start_date:
            if start_date < datetime.datetime.now():
                start_date = datetime.datetime.now()
        # Sees how many crawlers we need
        number_of_crawlers = 1
        if period_in_hours and end_date:
            crawling_time = end_date - start_date
            crawling_time_in_hours = crawling_time.total_seconds()/3600
            number_of_crawlers = \
                math.ceil(crawling_time_in_hours/period_in_hours) + 1
        # Creates time delayed crawlers
        _crawls = set()
        for i in range(0, number_of_crawlers):
            timedelta_hours = i * period_in_hours
            if number_of_crawlers > 1:
                this_start_date = \
                start_date + datetime.timedelta(hours=timedelta_hours)
            else:
                this_start_date = None
            crawl = Crawl(platform_name, strategy, parameters, campaign_id, 
                crawl_id, this_start_date)
            # Send the crawl to the platform
            platform.add_crawl_to_queue(crawl)
            # Add the crawl
            _crawls.add(crawl)
            self.crawls.add(crawl)
        self.campaign_ids.add(campaign_id)
        return [__crawl._id for __crawl in _crawls]

    def add_triple_store_crawl(self, triple_store_crawl_id):
        """ Gets crawl specifications from the triple store and adds it
        using the generic method """
        # Get the right parameters
        # use the generic add_crawl method
        # Returns the right number
        #
        # Not implemented yet, waiting for Nikos
        #
        return 500

    def stops_crawl(crawl_id):
        crawl = self.get_crawl(crawl_id)
        if not crawl: 
            return 404
        crawl = self.stops_crawl(crawl_id)
        return 200

    def rm_crawl(crawl_id):
        crawl = self.get_crawl(crawl_id)
        if not crawl:
            return 404
        if not (crawl.status == 'stopped'):
            return 400
        # platform
        platform = self.get_platform(crawl.platform_name)
        platform.rm_crawl(crawl)
        # self.crawls
        self.crawls.remove(crawl)
        # self.campaign_ids
        has_still_a_campaign_crawl = False
        for other_crawl in self.crawls:
            if other_crawl.campaign_id == crawl.campaign_id:
                has_still_a_campaign_crawl = True
                break
        if not has_still_a_campaign_crawl:
            self.campaign_ids.remove(crawl.campaign_id)
        # del object
        del crawl
        return 200

    #
    #   Methods not used by the pipeline
    #

    def get_crawl(crawl_id):
        """ Returns a crawl or None from a crawl_id """
        for crawl in self.crawls:
            if crawl._id == crawl_id:
                return crawl
        return None

    def get_campaign_crawls(self, campaign_id):
        """ Returns a crawl or None from a crawl_id """
        return [crawl for crawl in self.crawls if 
                crawl.campaign_id == campaign_id]

    def get_platforms_load(self):
        """ Returns load of the different platforms """ 
        return [platform.queue.qsize() for platform in self.platforms]

    def get_campaign_ids(self):
        """ Returns the campaign ids """
        return self.campaign_ids

    def get_platform(self, platform_name):
        """ Returns a platform object from a platform's name string """
        platform = False
        for _platform in self.platforms:
            if str(_platform.name) == str(platform_name):
                platform = _platform 
                break
        return platform
        

class CampaignStatistics:
    """ Statistics belonging to a campaign """
    def __init__(self):
        self.stat_dict = {}
        for platform in config.platforms:
            self.stat_dict.update(
                {
                    platform: {
                        'total_finished_crawls': 0,
                        'total_responses': 0,
                        'total_triples': 0,
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


class Platform:
    """ A platform corresponds to a service, e.g., a facebook platform for
    the facebook API service """
    def __init__(self, name, responses_handler):
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.queue = Queue.Queue()
        self.daemon_thread = Thread(target=self.platform_daemon)
        self.blender =  apiblender.Blender()
        self.responses_handler = responses_handler
        self.daemon_thread.start()

    def platform_daemon(self):
        self.logger.info('Starting %s daemon' % (self.name))
        while True:
            crawl = self.queue.get()
            # If it is not the time yet, it puts the crawl at the end of
            # the queue
            if crawl.start_date:
                if crawl.start_date > datetime.datetime.now():
                    self.queue.put(crawl)
                    continue
            # Else ..
            self.logger.info('[Starting crawl] %s' % crawl)
            crawl.run(self.blender, self.responses_handler)
            output_warc = self.responses_handler.warcs_handler.warc_file_path
            crawl.output_warc = output_warc
            self.logger.info('[Completed crawl] %s' % crawl)
            if (crawl.running_time < 5):
                logger.warning('Crawl duration < 5 seconds for %s' % 
                        crawl)

    def add_crawl_to_queue(self, crawl):
        self.queue.put(crawl)


class Crawl:
    """ A crawl is a wrapper for a spider execution """
    def __init__(self, platform_name, strategy, parameters, campaign_id, 
             crawl_id, start_date):
        # Id is set externally
        if crawl_id:
            self._id = crawl_id
        else:
            self._id = id(self)
        # Constituents
        self.platform_name = platform_name
        self.strategy = strategy
        self.parameters = parameters
        self.campaign_id = campaign_id
        self.start_date = start_date
        # Will be defined after it runs
        self.actual_start_date = None
        self.actual_end_date = None
        self.running_time = None
        self.output_warc = None
        # Status, starts as waiting
        self.set_status('waiting')
        # Creates the right spider
        spider_class = config.spider_mapping[(platform_name, strategy)]
        self.spider = eval(spider_class + '(parameters)')

    def set_status(self, status):
        """ Status can be 'waiting', 'running', 'stopped', 'finished',
        'being deleted' or 'deleted' """
        self.status = status

    def run(self, blender, responses_handler):
        # Pre-run
        self.actual_start_date = datetime.datetime.now()
        self.set_status('running')
        # Runs the crawl
        self.spider.run(blender, responses_handler)
        # Post-run 
        self.actual_end_date = datetime.datetime.now()
        self.set_status('finished')
        running_time = self.actual_end_date - self.actual_start_date 
        self.running_time = running_time.total_seconds()

    def __str__(self):
        return  "id: %s - running time in seconds: %s - strategy: %s -"\
                " parameters: %s - statistics: %s" % (self._id,
                        self.running_time, self.strategy,
                        self.parameters, self.spider.statistics)


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
        total_outlinks, total_triples = responses_handler.add_response(response)
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

class GooglePlusSearch(Spider):
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
