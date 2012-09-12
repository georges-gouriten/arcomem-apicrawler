import os

# Platforms dealt by the apicrawler
platforms = [ 'facebook', 'flickr', 'google_plus', 'twitter', 'youtube' ]

# File system constants
output_path = os.path.join(os.path.dirname(__file__), "output")
triples_path = os.path.join(output_path, 'triples')
outlinks_path = os.path.join(output_path, 'outlinks')
warcs_path = os.path.join(output_path, 'warcs')

# Ad hoc path in response data 
#
# Format is 'apiblender's service.interaction': 'key1.key2'
#
response_content_path = {
            'twitter-search.search': 'results',
            'youtube.search': 'feed.entry',
            'flickr.photos_search': 'photos.photo',
            'google_plus.activities_search': 'items',
            'facebook.search': 'data'
}
