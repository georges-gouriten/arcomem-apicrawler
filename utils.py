import json 

def jpath(item, path):
    for element in path.split('.'):
        try:
            item = item[element]
        except LookupError:
            #TODO: logging
            print "WARN: Could not find the right results with jpath"
            print '%s %s' % (item, path)
            return None
    return item






