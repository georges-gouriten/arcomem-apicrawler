import httplib
import json
import datetime
from arcomem_lib import config

local_connection = httplib.HTTPConnection( '0.0.0.0', 8080 )

# A test is a request defined by [info, path, method, body] 
tests = [
    [ 
        'Adding a simple crawl', '/crawl/add_direct', 'POST', 
        ['twitter', 'search', ['helium'], 'my_campaign']      
    ],
    [ 
        'Adding a list of crawls', '/crawl/add_direct', 'POST', 
        [
            ['facebook', 'search', ['helium'], 'my_campaign'],
            ['flickr', 'search', ['helium'], 'my_campaign'],
            ['google_plus', 'search', ['helium'], 'my_campaign'],
            ['youtube', 'search', ['helium','style'], 'my_campaign']
        ]
    ],
    [ 
        'Adding a crawl with start and end', '/crawl/add_direct', 'POST', 
        ['twitter', 'search', ['helium'], 'my_campaign',
            datetime.datetime.now().strftime(config.datetime_format),
            (datetime.datetime.now() + 
                datetime.timedelta(hours=2)).strftime(config.datetime_format),
            1, 'my_id']      
    ],
    [ 
        'Crawls information', '/crawls', 'GET', ''
    ],
    [ 
        'Stops crawl', '/crawl/my_id/stop', 'POST', ''
    ],
    [ 
        'Crawl information', '/crawl/my_id', 'GET', ''
    ],
    [ 
        'Deletes crawl', '/crawl/my_id', 'DEL', ''
    ],
    [ 
        'Crawls information', '/crawls', 'GET', ''
    ],
    [ 
        'Adding a crawl ending one hour ago', '/crawl/add_direct', 'POST', 
        ['twitter', 'search', ['helium'], 'my_campaign',
            None,
            (datetime.datetime.now() + 
            datetime.timedelta(hours=-1)).strftime(config.datetime_format),
            1]      
    ],
    [
        'Adding a crawl ending one hour ago', '/crawl/add_direct', 'POST', 
        ['twitter', 'search', ['helium'], 'my_campaign',
            None,
            (datetime.datetime.now() + 
            datetime.timedelta(hours=-1)).strftime(config.datetime_format),
            1]      
    ]
]

# Running the tests
for test in tests:
    print '-- %s --' % test[0]
    print 'Path: %s - Method: %s' % (test[1], test[2])
    print 'Body: %s' % (test[3])
    print
    body_str = json.dumps(test[3])
    local_connection.request(test[2], test[1], body_str)
    _response = local_connection.getresponse()
    _content = _response.read()
    print 'Return: '
    print _content
    print
