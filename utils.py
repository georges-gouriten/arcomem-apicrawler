import json 
import logging

def jpath(item, path):
    for element in path.split('.'):
        try:
            item = item[element]
        except LookupError:
            #TODO: logging
            logging.warning("Jpath invalid: %s %s" % (item, path))
            return None
    return item






