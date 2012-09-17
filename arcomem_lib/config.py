import os

#
#       Generic
#

# Platforms managed by the apicrawler
platforms = [ 'facebook', 'flickr', 'google_plus', 'twitter', 'youtube' ]

# Output directory
output_path = os.path.join(os.path.dirname(__file__), "output")

# Ad hoc paths in response data 
# Format is 'apiblender's service.interaction': 'key1.key2'
response_content_path = {
            'twitter-search.search': 'results',
            'youtube.search': 'feed.entry',
            'flickr.photos_search': 'photos.photo',
            'google_plus.activities_search': 'items',
            'facebook.search': 'data'
}

# Mapping platform, strategy to spider classes
spider_mapping = {
    ('facebook', 'search'):     'FacebookSearch',
    ('flickr', 'search'):       'FlickrSearch',
    ('google_plus', 'search'):  'GooglePlusSearch',
    ('youtube', 'search'):      'YoutubeSearch',
    ('twitter', 'search'):      'TwitterSearch'
}


#
#       Triples
#

# Triples directory
triples_path = os.path.join(output_path, 'triples')

# How often the triples rate will be logged in seconds
triples_rate_period = 300

# Size of the chunk sent to the triple store
triples_chunk_size=100000


#
#       WARCs
#

# WARCs directory
warcs_path = os.path.join(output_path, 'warcs')

# How ofter the WARCs rate will be logged in seconds
warcs_rate_period = 300

#
#       Outlinks
#

# Outlinks directory
outlinks_path = os.path.join(output_path, 'outlinks')
