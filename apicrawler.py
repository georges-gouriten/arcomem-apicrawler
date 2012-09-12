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
              '/crawl/([^/]+)/?', 'crawl',
              '/campaigns?/?', 'campaigns',
              '/campaign/([^/]+)/crawls/?', 'crawls',
              '/campaign/([^/]+)/?', 'campaign',
)
PLATFORMS = [ 'facebook', 'flickr', 'google_plus', 'twitter', 'youtube' ]

# Let's get is started 
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('apicrawler')
logger.info('Starting ARCOMEM APICrawler')
spiders_controller = spiders.SpidersController()

class crawl:
    def POST(self):
        str_data = web.data()
        crawls = json.loads(str_data)
        crawl_ids = []
        for crawl in crawls:
            crawl_id = spiders_controller.add_crawl(crawl)
            crawl_ids.append(crawl_id)
        return json.dumps(crawl_ids)
    
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
        return crawl_data


class campaigns:
    load = spiders_controller.get_load()
    def GET(self):
        campaign_names = spiders_controller.get_campaign_names()
        return json.dumps(campaign_names)


class campaign:
    def GET(self, campaign_name):
        campaign = spiders_controller.get_campaign(campaign_name)
        campaign_data = {}
        if campaign:
            campaign_data = {   "name": str(campaign.name),
                                "start_date": str(campaign.start_date),
                                "statistics": campaign.statistics.stats }
        return json.dumps(campaign_data)

    def DEL(self, campaign_name):
        spiders_controller.del_campaign(campaign_name)


class crawls:
    def GET(self, campaign_name):
        crawls = spiders_controller.get_crawls(campaign_name)
        crawls_data = []
        for crawl in crawls:
            crawl_data = {  "id": crawl._id,
                            "campaign": crawl.campaign.name,
                            "platform": crawl.platform.name,
                            "strategy": crawl.strategy,
                            "parameters": crawl.parameters,
                            "start_date": str(crawl.start_date),
                            "status": crawl.status,
                            "end_date": str(crawl.end_date),
                            "requests_count": crawl.requests_count }
            crawls_data.append(crawl_data)
        return json.dumps(crawls_data)


if __name__ == '__main__' :
    app = web.application(urls, globals())
    app.run()
