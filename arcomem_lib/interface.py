import datetime
import Queue
from threading import Thread
import logging
import math
import datetime

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
        """ Generic method to add a crawl to the queue, if a period and an
        end date is specified, it can add several crawlers. """
        # Gets the right platform (see below)
        platform = self.get_platform(platform_name)
        # Quick trick about the start date
        if start_date:
            # Converting from string to an actual date
            # Default conversion used
            start_date = datetime.datetime.strptime(start_date,
                                                    config.datetime_format)
            if start_date < datetime.datetime.now():
                start_date = datetime.datetime.now()
        # Sees how many crawlers we need
        number_of_crawlers = 1
        if period_in_hours and end_date:
            # Converting from string to an actual date
            # Default conversion used
            end_date = datetime.datetime.strptime(end_date,
                                                  config.datetime_format)
            crawling_time = end_date - start_date
            crawling_time_in_hours = crawling_time.total_seconds()/3600
            number_of_crawlers = \
                int(math.ceil(crawling_time_in_hours/period_in_hours)) + 1
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

    def stop_crawl(self, crawl_id):
        crawl = self.get_crawl(crawl_id)
        if not crawl:
            return 404
        # Else ..
        if crawl.status == 'waiting':
            crawl.status = 'stopped'
            return 200
        elif self.status == 'running':
            # Gives the stop order and waits
            crawl.spider.order_stop()
            _timeout = 90   # in seconds
            _step = 1
            _quantity = int(math.ceil(_timeout/_step)) + 1
            for i in range(0,_quantity):
                time.sleep(_step)
                if crawl.status == 'finished':
                    # Marks the crawl as stopped, the spider has probably not
                    # finished its crawl 
                    crawl.status = 'stopped'
                    return 200
            # If it did not work given this timeout, we have a problem
            return 405
        else:
            # Makes not much sense to stop what is already stopped
            return 406

    def rm_crawl(self, crawl_id):
        crawl = self.get_crawl(crawl_id)
        if not crawl:
            return 404
        if not (crawl.status == 'stopped'):
            return 400
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

    def get_crawl(self, crawl_id):
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
            # Do not process stopped crawl
            if crawl.status == 'stopped':
                return
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
        self.spider = eval('spiders.' + spider_class + '(parameters)')

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

