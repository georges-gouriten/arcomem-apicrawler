import interface

# Ad hoc and basic testing at the moment.
#
# IDEA: use unittest python library
#

if __name__ == '__main__':
    apic_interface = interface.APICrawlerInterface()
    interface.add_crawl([   'twitter',
                            'search',
                            ['green', 'peace'],
                            'my_campaign_is_awesome'])
