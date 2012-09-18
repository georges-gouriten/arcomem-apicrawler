import web
from web import form
import json
import logging
import logging.config 

from arcomem_lib import interface
from arcomem_lib import config

web.config.debug = False
render = web.template.render('templates/')
urls = (
              '/crawl/add_from_triple_store/?', 'add_crawl_from_triple_store',
              '/crawl/add_direct/?', 'add_crawl_directly',
              '/crawl/([^/]+)/?', 'crawl_information_or_deletion',
              '/crawl/([^/]+)/stop/?', 'stop_crawl',
              '/crawls/?', 'crawls_information'
              #     Deprecated
              #'/campaigns?/?', 'campaigns',
              #'/campaign/([^/]+)/crawls/?', 'crawls',
              #'/campaign/([^/]+)/?', 'campaign',
)
PLATFORMS = [ 'facebook', 'flickr', 'google_plus', 'twitter', 'youtube' ]

# Let's get started 
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('apicrawler')
logger.info('Starting ARCOMEM APICrawler')
apicrawler_interface = interface.APICrawlerInterface()

#
# HTTP Response classes
#

class add_crawl_from_triple_store:
    def POST(self):
        """ Adds a crawl or a list of crawls fetching the parameters from the
        triple store """
        str_data = web.data()
        try:
            crawls_data = json.loads(str_data)
        except Exception as e:
            raise NonJSON, e
        try:
            crawl_id = str(crawls_data['crawl_id'])
        except Exception as e:
            raise WrongFormat, e
        status = apicrawler_interface.add_triple_store_crawl(crawl_id)
        if status == 404:
            raise NoCrawlInTripleStore
        elif status == 200:
            return 'OK'
        else:
            raise UnknownError


class add_crawl_directly:
    def POST(self):
        """ Adds a crawl or a list of crawls directly passing the
        parameters """
        str_data = web.data()
        try:
            crawls_data = json.loads(str_data)
        except Exception as e:
            raise NonJSON, e
        if type(crawls_data[0]) is list:
            crawls_ids = []
            for crawl_data in crawls_data:
                try:
                    crawl_ids = apicrawler_interface.add_crawl(*crawl_data)
                except Exception as e:
                    raise WrongFormat, e
                crawls_ids.append(crawl_ids)
            return json.dumps(crawls_ids, sort_keys=True, indent=4)
        else:
            try:
                crawl_ids = apicrawler_interface.add_crawl(*crawls_data)
            except Exception as e:
                raise WrongFormat, e
            return json.dumps(crawl_ids, sort_keys=True, indent=4)

class crawl_information_or_deletion:    
    def GET(self, crawl_id):
        """ Returns crawl information """
        crawl = apicrawler_interface.get_crawl(crawl_id) 
        if not crawl:
            raise CrawlNotFound, crawl_id
        # Else ..
        return str(crawl) 

    def DEL(self, crawl_id):
        """ Deletes crawl """
        status = apicrawler_interface.rm_crawl(crawl_id)
        if status == 200:
            return 'OK'
        elif status == 404:
            raise CrawlNotFound, crawl_id
        elif status == 400:
            raise CrawlNotStopped, crawl_id
        else:
            raise UnknownError

class stop_crawl:
    def POST(self, crawl_id):
        """ Stops crawl """
        status = apicrawler_interface.stop_crawl(crawl_id)
        if status == 200:
            return 'OK'
        elif status == 113:
            return 'Crawl was already stopped or finished!'
        elif status == 404:
            raise CrawlNotFound, crawl_id
        elif status == 500:
            raise CrawlCouldNotStop, crawl_id
        else:
            raise UnknownError



class crawls_information:
    def GET(self):
        """ Returns all crawls information """
        crawls = apicrawler_interface.crawls
        crawls_str = '[\n'
        for crawl in crawls:
            crawls_str += str(crawl) + ',\n'
        crawls_str = crawls_str.rstrip(',\n')
        crawls_str += '\n]'
        return crawls_str

#
#       Error classes
#

#Wrapper
class GenericError(web.HTTPError):
    def __init__(self, status, error_name, error_data):
        headers = {'Content-Type': 'text/html'}
        data = json.dumps([error_name, error_data], sort_keys=True,
                          indent=4)
        web.HTTPError.__init__(self, status, headers, data)


class NonJSON(GenericError):
    def __init__(self, e):
        status = '400 non JSON'
        error_data = 'Could not parse data, please check it is proper'\
                     'JSON.' 
        error_data += ' - Python error: %s' 
        GenericError.__init__(self, status, self.__class__.__name__,
                              error_data)


class WrongFormat(GenericError):
    def __init__(self, e):
        status = '400 wrong format'
        error_data = 'Wrong data format, please check your format with'\
                     ' the API description'
        error_data += ' - Python error: %s' % e
        GenericError.__init__(self, status, self.__class__.__name__,
                              error_data)


class NoCrawlInTripleStore(GenericError):
    def __init__(self):
        status = '404 crawl not found in the triple store'
        error_data = 'Could not find the crawl specs in the triple store'
        GenericError.__init__(self, status, self.__class__.__name__,
                              error_data)


class CrawlNotFound(GenericError):
    def __init__(self, crawl_id):
        status = '404 crawl not found'
        error_data = 'Could not find the crawl: %s' % crawl_id
        GenericError.__init__(self, status, self.__class__.__name__,
                              error_data)


class CrawlCouldNotStop(GenericError):
    def __init__(self, crawl_id):
        status = '500 crawl could not stop'
        error_data = 'We tried but we failed, please try again to stop'\
                     ' %s' % crawl_id
        GenericError.__init__(self, status, self.__class__.__name__,
                              error_data)
  

class CrawlNotStopped(GenericError):
    def __init__(self, crawl_id):
        status = '400 crawl is not stopped'
        error_data = 'Cannot delete %s because its status is not on stopped'\
                     % crawl_id
        GenericError.__init__(self, status, self.__class__.__name__,
                              error_data)


class UnknownError(GenericError):
    def __init__(self):
        status = '500 unknow error'
        error_data = 'Unknown Error, this should not happen!'
        GenericError.__init__(self, status, self.__class__.__name__,
                              error_data)


#
#        Deprecated response classes 
#

#
#class campaigns:
#    load = spiders_controller.get_load()
#    def GET(self):
#        campaign_names = spiders_controller.get_campaign_names()
#        return json.dumps(campaign_names)
#
#
#class campaign:
#    def GET(self, campaign_name):
#        campaign = spiders_controller.get_campaign(campaign_name)
#        campaign_data = {}
#        if campaign:
#            campaign_data = {   "name": str(campaign.name),
#                                "start_date": str(campaign.start_date),
#                                "statistics": campaign.statistics.stats }
#        return json.dumps(campaign_data)
#
#    def DEL(self, campaign_name):
#        spiders_controller.del_campaign(campaign_name)
#
#

if __name__ == '__main__' :
    app = web.application(urls, globals())
    app.run()
