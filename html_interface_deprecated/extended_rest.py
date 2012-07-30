import web
from web import form
import spiders

spiders_controller = spiders.SpidersController()
render = web.template.render('templates/')
urls = (
          '/', 'index',
          '/new_crawls', 'new_crawls',
          '/campaigns?/?', 'campaigns',
          '/campaign/([^/]+)/crawls/?', 'crawls',
          '/campaign/([^/]+)/?', 'campaign',
)

PLATFORMS = [ 'facebook', 'flickr', 'google_plus', 'twitter', 'youtube' ]

class index:
    def GET(self):
        load = spiders_controller.get_load()
        return render.index(load) 

new_crawl_form = form.Form( 
    form.Textbox("campaign_name", form.notnull),
    form.Textbox("keywords", form.notnull),
    form.Checkbox('twitter'), 
    form.Checkbox('facebook'), 
    form.Checkbox('youtube'), 
    form.Checkbox('flickr'), 
    form.Checkbox('google_plus'))

def extract_keywords(form):
    keywords = form['keywords'].value.split(',')
    return keywords


class new_crawls: 
    def GET(self): 
        form = new_crawl_form()
        return render.new_crawls(form)

    def POST(self): 
        form = new_crawl_form() 
        if not form.validates(): 
            return render.new_crawls(form)
        else:
            keywords = extract_keywords(form)
            campaign_name = form['campaign_name'].value
            formdata = web.input()
            for PLATFORM in PLATFORMS:
                if formdata.has_key(PLATFORM):
                    spiders_controller.new_crawl(campaign_name, \
                            PLATFORM, keywords)
            return "Crawls have been successfully created!"


class campaigns:
    def GET(self):
        campaign_names = spiders_controller.get_campaign_names()
        if campaign_names:
            return render.campaigns(campaign_names)
        else:
            return 'No campaign has been created yet!'

class campaign:
    def GET(self, campaign_name):
        campaign = spiders_controller.get_campaign(campaign_name)
        if campaign:
            return render.campaign(campaign)
        else:
            return 'Campaign ' + campaign_name + ' not found'

class crawls:
    def GET(self, campaign_name):
        crawls = spiders_controller.get_crawls(campaign_name)
        if crawls:
            return render.crawls(campaign_name, crawls)
        else:
            return 'Crawls not found for campaign: ' + campaign_name

if __name__ == '__main__' :
    app = web.application(urls, globals())
    app.run()
