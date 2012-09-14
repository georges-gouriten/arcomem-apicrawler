import web
from web import form
import json
import logging
import logging.config 

from arcomem_lib import spiders

web.config.debug = False
render = web.template.render('templates/')
urls = (
              '/crawl/add', 'crawl',
              '/crawl/add_direct', 'crawl_direct',
              '/crawl/([^/]+)/?', 'crawl',
              '/crawls/?', 'crawls',
              '/campaigns?/?', 'campaigns',
              '/campaign/([^/]+)/crawls/?', 'crawls',
              '/campaign/([^/]+)/?', 'campaign',
)
PLATFORMS = [ 'facebook', 'flickr', 'google_plus', 'twitter', 'youtube' ]

# Let's get is started 
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('apicrawler')
logger.info('Starting ARCOMEM APICrawler')
apicrawler_interface = spiders.APICrawlerInterface()

# Error classes
class FourHundred(web.HTTPError):
    def __init__(self, choices):
        status = '400'
        headers = {'Content-Type': 'text/html'}
        data = ''
        web.HTTPError.__init__(self, status, headers, data)


class FourHundredAndFour(web.HTTPError):
    def __init__(self, choices):
        status = '404'
        headers = {'Content-Type': 'text/html'}
        data = ''
        web.HTTPError.__init__(self, status, headers, data)

class FiveHundred(web.HTTPError):
    def __init__(self, choices):
        status = '500'
        headers = {'Content-Type': 'text/html'}
        data = ''
        web.HTTPError.__init__(self, status, headers, data)

# Response classes
class crawl:
    # Adds crawl fetching the parameters from the triple store
    def POST(self):
        str_data = web.data()
        crawl = json.loads(str_data)
        crawl_id = crawl['crawl_id'] 
        status = apicrawler_interface.add_triple_store_crawl(crawl_id)
        if status == 404:
            raise FourHundredAndFour
        elif status == 200:
            return 'OK'
        else:
            return 500
    
    def GET(self, crawl_id):
        crawl = spiders_controller.object_from_id(crawl_id) 
        crawl_data = {}
        if crawl:
            crawl_data = {  "id": crawl._id,
                    "campaign": crawl.campaign.name,
                    "platform": crawl.platform.name,
                    "strategy": crawl.strategy,
                    "parameters": crawl.parameters,
                    "start_date": str(crawl.start_date),
                    "status": crawl.status,
                    "end_date": str(crawl.end_date),
                    "requests_count": crawl.requests_count }
        return json.dumps(crawl_data, sort_keys=True, indent=4)

class crawl_direct:
    # Adds crawl directly passing the parameters
    def POST(self):
        str_data = web.data()
        crawls_data = json.loads(str_data)
        crawls_ids = []
        for crawl_data in crawls_data:
            crawl_ids = apicrawler_interface.add_crawl(*crawl_data)
            crawls_ids.append(crawl_ids)
        return json.dumps(crawls_ids, sort_keys=True, indent=4)

class crawls:
    def GET(self):
        crawls = apicrawler_interface.crawls
        crawls_data = []
        for crawl in crawls:
            crawl_data = {  "id": crawl._id,
                            "state": crawl.status,
                            "output_warc": crawl.output_warc,
                            "campaign": crawl.campaign_id,
                            "platform": crawl.platform_name,
                            "strategy": crawl.strategy,
                            "parameters": crawl.parameters,
                            "start_date": str(crawl.start_date),
                            "actual_start_date": str(crawl.actual_start_date),
                            "actual_end_date": str(crawl.actual_end_date),
                            "running_time": crawl.running_time,
                            "statistics": crawl.spider.statistics
                        }
            crawls_data.append(crawl_data)
        return json.dumps(crawls_data, sort_keys=True, indent=4)


#
#        Not used
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
