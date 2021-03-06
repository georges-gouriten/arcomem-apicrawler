import datetime
import Queue
from threading import Thread
import logging
import math
import datetime
import time
import json

import apiblender

import config
import responses
import spiders

""" 
This module is about the interface of the APICrawler to the external world.
"""

# 
# IDEA: this module could maybe be splitted, Platform and Crawl could be put
# somewhere else. Not a big deal though.
#

logger = logging.getLogger('apicrawler')

class APICrawlerInterface:
    """ Interface to the external world """
    def __init__(self):
        # Dummy call of strptime (thread bug)
        datetime.datetime.strptime('2012-01-01', '%Y-%m-%d')
        logger.info('Starting the APICrawler')
        self.responses_handler = responses.ResponsesHandler()
        # Containers
        self.crawls = set()
        self.campaign_ids = set() 
        self.platforms = set()
        # Initializes platforms
        for platform_str in config.platforms:
            logger.info('Starting platform %s' % platform_str)
            platform = Platform(platform_str, self.responses_handler)
            self.platforms.add(platform)
        logger.info('APICrawler is ready')

    def add_crawl(  self, 
                    platform_name,          # String, e.g. twitter
                    strategy,               # String, e.g. search 
                    parameters,             # Depends on the spider
                    campaign_id,            # String
                    start_date=None, 
                    end_date=None, 
                    period_in_hours=0,      # If > 0, repeats every x hours
                    crawl_id=None           # If already defined, o/w
                                            # generated
                ):
        """ Generic method to add a crawl to the API Crawler """
        # Creates the crawler
        new_crawl = Crawl(platform_name, strategy, parameters,
                campaign_id, start_date, end_date, period_in_hours,
                crawl_id)
        # New spiders are created (cf. Crawl __init___)
        new_spiders = new_crawl.spiders
        # Gets the right platform (see below)
        platform = self.get_platform(platform_name)
        # Add the spiders to the platform 
        for new_spider in new_spiders:
            platform.add_spider_to_queue(new_spider)
        # Add the crawl and the campaign_id
        self.crawls.add(new_crawl)
        self.campaign_ids.add(campaign_id)
        # Returns the crawl_id
        return new_crawl._id

    def add_triple_store_crawl(self, triple_store_crawl_id):
        """ Gets crawl specifications from the triple store and adds it
        using the generic method """
        # Get the right parameters
        # use the generic add_crawl method
        # Returns the right number
        #
        # TODO: Implements Nikos method
        #
        return 500

    def stop_crawl(self, crawl_id):
        """ Stops crawl """
        crawl = self.get_crawl(crawl_id)
        if not crawl:
            return 404
        # Else ..
        http_status = crawl.stop_crawl()
        return http_status

    #
    #   IDEA: removed crawls could be dumped somewhere
    #
    def rm_crawl(self, crawl_id):
        """ Deletes crawl """
        crawl = self.get_crawl(crawl_id)
        if not crawl:
            return 404
        for spider in crawl.spiders:
            if not (spider.status == 'stopped' or 'finished'):
                return 400
        # Else ..
        for spider in crawl.spiders:
            spider.status = 'being removed'
            del spider
        self.crawls.remove(crawl)
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

    def get_crawl(self, crawl_id):
        """ Returns a crawl or None from a crawl_id """
        for crawl in self.crawls:
            if crawl._id == crawl_id:
                return crawl
        return None

    def get_platform(self, platform_name):
        """ Returns a platform object from a platform's name string """
        platform = False
        for _platform in self.platforms:
            if str(_platform.name) == str(platform_name):
                platform = _platform 
                break
        return platform
        
#
#       Currently not served by the Web interface 
#
#       IDEA: Add campaign considerations

    def get_campaign_crawls(self, campaign_id):
        """ Returns crawls from a campaign_id """
        return [crawl for crawl in self.crawls if 
                crawl.campaign_id == campaign_id]


    def get_campaign_ids(self):
        """ Returns the campaign ids """
        return self.campaign_ids

    def get_platforms_load(self):
        """ Returns load of the different platforms """ 
        return [platform.queue.qsize() for platform in self.platforms]


#class CampaignStatistics:
#    """ Statistics belonging to a campaign """
#    def __init__(self):
#        self.stat_dict = {}
#        for platform in config.platforms:
#            self.stat_dict.update(
#                {
#                    platform: {
#                        'total_finished_crawls': 0,
#                        'total_responses': 0,
#                        'total_triples': 0,
#                        'total_outlinks': 0
#                    }
#                }   
#            )
#
#    def add_crawl_statistics(self, statistics, platform_name):
#        total_responses = statistics['total_responses']
#        total_outlinks = statistics['total_outlinks']
#        total_triples = statistics['total_triples']
#        self.stat_dict[platform_name]['total_finished_crawls'] += 1
#        self.stat_dict[platform_name]['total_responses'] += total_responses
#        self.stat_dict[platform_name]['total_triples'] += total_triples
#        self.stat_dict[platform_name]['total_outlinks'] += total_outlinks 
#

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
        """ Loops and executes spiders """
        self.logger.info('Starting %s daemon' % (self.name))
        while True:
            spider = self.queue.get()
            # Processes only waiting spiders 
            if not spider.status == 'waiting':
                return
            # If it is not the time yet, puts the spider at the end of
            # the queue
            if spider.start_date:
                if spider.start_date > datetime.datetime.now():
                    self.queue.put(spider)
                    continue
            # Does not process if the end_date is passed
            if spider.end_date:
                if spider.end_date < datetime.datetime.now():
                    continue
            # Else ..
            self.logger.info('[Starting spider] id: %s' % id(spider))
            spider.wrapper_run(self.blender, self.responses_handler)
            output_warc = self.responses_handler.warcs_handler.warc_file_path
            spider.output_warc = output_warc
            self.logger.info('[Completed spider] \n%s' % spider)
            if (spider.running_time < 5):
                logger.warning('Spider duration < 5 seconds for %s' % 
                        spider)

    def add_spider_to_queue(self, spider):
        """ Adds a spider to the platform's queue """
        self.queue.put(spider)

class Crawl:
    """ A crawl is mostly a container for one or several spiders. """
    def __init__(self, platform_name, strategy, parameters, campaign_id, 
                 start_date, end_date, period_in_hours, crawl_id):
        # Id can be set externally
        if crawl_id:
            self._id = crawl_id
        else:
            self._id = id(self)
        # Constituents
        self.platform_name = platform_name
        self.strategy = strategy
        self.parameters = parameters
        self.campaign_id = campaign_id
        # Start date
        if start_date:
            # Converting from string to a datetime object
            # Default conversion used
            start_date = datetime.datetime.strptime(start_date,
                                                    config.datetime_format)
            if start_date < datetime.datetime.now():
                start_date = datetime.datetime.now()
                logger.warning('Start date for crawl id: %s is old, replaced' 
                               ' by current datetime' % self._id)
        else:
            start_date = datetime.datetime.now()
        self.start_date = start_date
        # End date
        if end_date:
            # Converting from string to a datetime object
            # Default conversion used
            end_date = datetime.datetime.strptime(end_date,
                                                  config.datetime_format)
            if start_date >= end_date:
                logger.warning('Start date >= End date, ignoring end date'
                               ' information for crawler id: %s' % self._id)
                end_date = None
        self.end_date = end_date
        # Period in hours
        if not period_in_hours:
            self.period_in_hours = None
        elif int(period_in_hours) > 0:
                self.period_in_hours = int(period_in_hours)
        else:
            logger.warning('Period in hours has to be None or > 0. Ignoring period'
                       ' for crawler %s' % self._id)
            self.period_in_hours = None
        # Creating the spiders
        self.create_spiders()

    def create_spiders(self):
        """ Create one or more spiders depending on start date, end date and period """
        # Sees how many spiders we need
        number_of_spiders = 1
        if self.period_in_hours and self.start_date and self.end_date:
                crawling_time = self.end_date - self.start_date
                crawling_time_in_hours = crawling_time.total_seconds()/3600
                number_of_spiders = \
                    int(math.ceil(crawling_time_in_hours/self.period_in_hours)) + 1
        # Creates one or more spiders
        self.spiders = []
        for i in range(0, number_of_spiders):
            if number_of_spiders > 1:
                timedelta_hours = i * self.period_in_hours
                this_start_date = \
                self.start_date + datetime.timedelta(hours=timedelta_hours)
            else:
                this_start_date = self.start_date
            # Creates the right spider
            spider_class = config.spider_mapping[(self.platform_name, self.strategy)]
            new_spider = eval('spiders.' + spider_class +
                '(self.parameters, this_start_date, self.end_date)')
            self.spiders.append(new_spider)
        
    def stop_crawl(self):
        """ Stops crawl """
        http_status = 0
        for spider in self.spiders:
            spider_http_status = self.stop_spider(spider)
            # Returns the worst status
            http_status = max(http_status, spider_http_status)
        return http_status

    def stop_spider(self, spider):
        """ Stops spider """
        if spider.status == 'waiting':
            spider.status = 'stopped'
            return 200
        # Else ..
        elif spider.status == 'running':
            # Gives the stop order and waits
            spider.stop_now = True
            _timeout = 90   # in seconds
            _step = 1
            _quantity = int(math.ceil(_timeout/_step)) + 1
            for i in range(0,_quantity):
                time.sleep(_step)
                if spider.status == 'finished':
                    # Marks the spider as stopped, the spider has probably not
                    # finished its spider 
                    spider.status = 'stopped'
                    return 200
            # If it did not work given this timeout, we have a problem
            return 500
        else:
            # Makes not much sense to stop what is already stopped
            return 113

    def get_dict(self):
        """ Returns a JSON friendly dict of the interesting attributes """
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
        spiders_list = []
        for spider in self.spiders:
            spiders_list.append(spider.get_dict())
        return {
                "campaign_id": self.campaign_id,
                "id": self._id,
                "dates": {
                    "start_date": start_date_str,
                    "end_date": end_date_str
                },
                "platform": self.platform_name,
                "strategy": self.strategy,
                "parameters": self.parameters,
                "spiders": spiders_list
               }

    def __str__(self):
        return json.dumps(self.get_dict(), indent=4, sort_keys=True)
