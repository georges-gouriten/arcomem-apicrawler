import spiders

# Ad hoc testing at the moment.
# TODO: use unittest python library

sc = spiders.SpidersController()
sc.add_crawl([  'my_campaign_name',
                'twitter',
                'strategy_does_not_matter',
                ['green', 'peace'] ])
