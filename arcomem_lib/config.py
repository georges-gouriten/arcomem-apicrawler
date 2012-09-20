""" Contains the configuration parameters of the different modules """

import os

####################################
####            Generic         ####
####################################

# Platforms managed by the apicrawler
platforms = [ 'facebook', 'flickr', 'google_plus', 'twitter', 'youtube' ]

# String format for date-time
datetime_format = '%Y-%m-%d_%H:%M:%S'

# Output directory
output_path = os.path.join(os.path.dirname(__file__), "output")

# Paths to content items in response data
# Format is (server, interaction): 'key1.key2. ... .keyN'
response_content_paths = { 
    ('facebook', 'search'):                 'data',
    ('facebook', 'users'):                  '',
    ('flickr', 'photos_search'):            'photos.photo',
    ('google_plus', 'activities_search'):   'items',
    ('youtube', 'search'):                  'feed.entry',
    ('twitter-search', 'search'):           'results'
}

# Mapping platform, strategy to spider classes
spider_mapping = {
    ('facebook', 'search'):     'FacebookSearch',
    ('facebook', 'users'):      'FacebookUsers',
    ('flickr', 'search'):       'FlickrSearch',
    ('google_plus', 'search'):  'GooglePlusSearch',
    ('youtube', 'search'):      'YoutubeSearch',
    ('twitter', 'search'):      'TwitterSearch'
}

####################################
####            Triples         ####
####################################

# Triples directory
triples_path = os.path.join(output_path, 'triples')

# How often the triples rate will be logged in seconds
triples_rate_period = 300

# Size of the chunk sent to the triple store
triples_chunk_size=100000


####################################
####            WARCs           ####
####################################

# WARCs directory
warcs_path = os.path.join(output_path, 'warcs')

# How ofter the WARCs rate will be logged in seconds
warcs_rate_period = 300


####################################
####            Outlinks        ####
####################################

# Outlinks directory
outlinks_path = os.path.join(output_path, 'outlinks')

# Size of the chunk sent to the crawler
outlinks_chunk_size = 50000
