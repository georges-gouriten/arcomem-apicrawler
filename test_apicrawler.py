import httplib
import json

local_connection = httplib.HTTPConnection( '0.0.0.0', 8080 )

# Adding some crawls
# Testing one by one crawl
crawls_data = [
        'twitter',
        'search',
        ['helium'],
        'my_campaign_id'
        ]
crawls_data_str = json.dumps(crawls_data)
local_connection.request('POST', '/crawl/add_direct', crawls_data_str)
_response = local_connection.getresponse()
_content = _response.read()
print _content

# Testing list
# Start and end date
# Crawls information

#crawl_data =

# Crawls information

# Stopping a crawl

# Deleting a crawl
