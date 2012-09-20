""" Tests the main calls of the API Crawler HTTP interface. A detailed
output is available in test.log """ 
#
# Ad hoc and basic testing at the moment
# IDEA: use python unittest library
#
import httplib
import json
import datetime
from arcomem_lib import config


# A test is a request defined by [info, path, method, body, expected_status] 
tests = [
    [ 
        'Adding a simple crawl', '/crawl/add_direct', 'POST', 
        ['facebook', 'users', ['helium'], 'my_campaign'], 200      
    ],
    [ 
        'Adding a list of crawls', '/crawl/add_direct', 'POST', 
        [
            ['facebook', 'search', ['helium'], 'my_campaign'],
            ['flickr', 'search', ['helium'], 'my_campaign'],
            ['google_plus', 'search', ['helium'], 'my_campaign'],
            ['youtube', 'search', ['helium','style'], 'my_campaign']
        ], 200
    ],
    [ 
        'Adding a crawl with start and end', '/crawl/add_direct', 'POST', 
        ['twitter', 'search', ['helium'], 'my_campaign',
            None,
            (datetime.datetime.now() + 
                datetime.timedelta(hours=2)).strftime(config.datetime_format),
            1, 'my_id'], 200
    ],
    [ 
        'Crawls information', '/crawls', 'GET', '', 200
    ],
    [ 
        'Stops crawl', '/crawl/my_id/stop', 'POST', '', 200
    ],
    [ 
        'Crawl information', '/crawl/my_id', 'GET', '', 200
    ],
    [ 
        'Deletes crawl', '/crawl/my_id', 'DEL', '', 200
    ],
    [ 
        'Crawls information', '/crawls', 'GET', '', 200
    ],
    [ 
        'Adding a crawl ending one hour ago', '/crawl/add_direct', 'POST', 
        ['twitter', 'search', ['helium'], 'my_campaign',
            None,
            (datetime.datetime.now() + 
            datetime.timedelta(hours=-1)).strftime(config.datetime_format),
            1], 200      
    ]
]

if __name__ == '__main__':
    local_connection = httplib.HTTPConnection( '0.0.0.0', 8080 )
    # Running the tests
    for test in tests:
        print '-- %s --' % test[0]
        print 'Path: %s - Method: %s' % (test[1], test[2])
        print 'Body: %s' % (test[3])
        print 'Expected status: %s' % test[4]
        print
        body_str = json.dumps(test[3])
        local_connection.request(test[2], test[1], body_str)
        _response = local_connection.getresponse()
        _content = _response.read()
        print 'Actual status: %s' % _response.status
        print 'Success: %s' % (_response.status == test[4])
        print
        with open('test.log', 'a') as test_file:
            test_file.write('** Test **: %s\n' % test)
            test_file.write('** Date **: ' +\
                    datetime.datetime.now().strftime(config.datetime_format))
            test_file.write('\n** Response **: %s\n\n\n' % _content)
