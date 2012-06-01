import Queue
import socket
import json
import time
import datetime
import re
from threading import Thread
import sys

import warc

RESULTS = {
    'twitter-search.search': 'prepared_content.results',
    'youtube.search': 'feed.entry',
    'flickr.photos_search': 'photos.photo',
    'google_plus.activities_search': 'items',
    'facebook.search': 'data'
}

class ResponsesHandler: 

    def __init__(self): 
        self.triples_handler = TripleManager() 
        self.warcs = []

    def add_response(self, response):
        # Extracting result or results
        result = self.manual_path(response)
        triple_prefix = response['triple_prefix']
        if type(result) is list:
            for _result in result:
                try:
                    str(_result['id'])
                except Exception:
                    print 'ERROR in processing the output: %s has no ID' %\
                        (_result)
                    continue
                self.write_warc(_result, triple_prefix, response['headers'])
                self.triples_handler.receive_result(_result, triple_prefix)
        else:
            self.write_warc(result, triple_prefix, response['headers'])
            self.triples_handler.receive_result(result, triple_prefix)

    def write_warc(self, result, triple_prefix, headers):
        self.warcs.append((result, triple_prefix, headers))
        # Triggered every 0.5 GB = 536870912 Bytes
        if sys.getsizeof(self.warcs) > 500000000:
            warc_name = "apicrawler.%s.warc.gz" % (datetime.datetime.now())
            print "Writing warc file: %s" % warc_name
            warc_file = warc.open("warcs/" + warc_name, "w")
            # WARCInfo record
            warc_header = warc.WARCHeader({
                            "WARC-Type": "warcinfo",
                            "Content-Type": "application/warc-fields",
                            "WARC-Filename": warc_name},\
                            defaults = True)
            warc_payload = 'software: apicrawler\nhostname: ia200127'
            warc_record = warc.WARCRecord(warc_header, warc_payload)
            warc_file.write_record(warc_record)
            warcinfo_id = warc_header['WARC-RECORD-ID']
            for _warc in self.warcs:
                # Object record
                (_result, _triple_prefix, _headers) = _warc
                target_uri = "%s%s" % (_triple_prefix, _result['id']) 
                warc_header = warc.WARCHeader({
                    "WARC-Type": "api-object",
                    "Content-Type": "application/json",
                    "WARC-Warcinfo-ID": warcinfo_id,
                    "WARC-Target-URI": target_uri,
                    "WARC-Identified-Payload-Type": "application/json"},\
                    defaults = True ) 
                warc_payload = str({ 'data': _result, 'headers': _headers })
                warc_record = warc.WARCRecord(warc_header, warc_payload)
                warc_file.write_record(warc_record)
                # Metadata record
                concurrent_to = warc_header['WARC-RECORD-ID']
                warc_header = warc.WARCHeader({
                    "WARC-Type": "metadata",
                    "Content-Type": "application/warc-fields",
                    "WARC-Warcinfo-ID": warcinfo_id,
                    "WARC-Target-URI": target_uri,
                    "WARC-Concurrent-To": concurrent_to,
                    "WARC-Identified-Payload-Type": "application/json"},\
                    defaults = True )
                init_outlinks = set()
                total_outlinks = list(  self.extract_outlinks(  _result,\
                                                                init_outlinks))
                warc_payload = str({    "parent": str(target_uri),
                                    "outlinks": str(total_outlinks) })
                warc_record = warc.WARCRecord(warc_header, warc_payload)
                warc_file.write_record(warc_record)
            del self.warcs[:]

    def manual_path(self, response):
        result = response
        for item in RESULTS:
            found = False
            for key in RESULTS[item].split('.'):
                try:
                    result = result[key]
                    found = True
                except Exception:
                    found = False
                    break
            if found:
                return result

    def extract_outlinks(self, _result, outlinks):
        if type(_result) is dict:
            for key in _result.keys():
                self.extract_outlinks(_result[key], outlinks)
        elif type(_result) is list:
            for item in _result:
                self.extract_outlinks(item, outlinks)
        else:
            try:
                str_result = str(_result) 
            except Exception:
                return outlinks
            for item in re.findall("https?://[^ ]+", str_result, re.I):
                outlinks.add(item)
        return outlinks


class TripleManager:
    
    def __init__(self):
        self._triples = Queue.Queue()
        self.s = None
        self.start_daemon()

    def start_daemon(self):
        t = Thread(target=self.triples_daemon)
        t.start() 

    def triples_daemon(self): 
        print 'Triples_daemon active.'
        while True:
            time.sleep(10)
            self.initiate_socket_connection()
            self.send_chunk_to_socket()
            self.close_socket()
   
    def receive_result(self, result, triple_prefix):
        triples = self.make_triples(result, triple_prefix)
        for triple in triples:
            self._triples.put(triple)

    def make_triples(self, result, triple_prefix):
       triples = []
       subject = triple_prefix + str(result['id'])
       predicates_objects = self.flatten(result, '')
       for predicate_object in predicates_objects:
           triples.append([ subject, 
                            predicate_object[0],
                            predicate_object[1] ])
       return triples

    def flatten(self, item, predicate):
        #[[predicate, object]]
        po = list()
        if type(item) is dict:
            for key in item.keys():
                new_predicate = predicate + key + '_'
                new_item = item[key]
                sub_po = self.flatten(new_item, new_predicate)
                po.extend(sub_po)
        elif type(item) is list:
            for new_item in item:
                sub_po = self.flatten(new_item, predicate)
                po.extend(sub_po)
        else:
            po.append([predicate.strip('_'), item])
        return po

    def initiate_socket_connection(self, host='localhost', port=19898):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((host, port))

    # TODO: see when it should be sent
    def send_chunk_to_socket(self, quantity=1000000):
        chunk = []
        for i in range(0, quantity-1):
            try:
                triple = self._triples.get(False)
            except Queue.Empty:
                break
            chunk.append(triple)
        if chunk:
            string_chunk = self.stringify_triples(chunk)
            print 'sending %s' % string_chunk[0:75]
            self.s.send(string_chunk)

    def close_socket(self):
        self.s.close()

    def stringify_triples(self, triples):
        string_triples = []
        for triple in triples:
            string_triple = []
            for position, item  in enumerate(triple):
                if position < 2:
                    item = 'apicrawler:' + json.dumps(item)
                else:
                    item = json.dumps(item)
                string_triple.append(item.replace('"',''))
            string_triple = "\\SPO".join(string_triple)
            string_triples.append(string_triple)
        string_triples = "\\EOT".join(string_triples)
        string_triples += "\\EOL"
        return string_triples

#		#Sending results to the triple store stringTriples =
#		utils.stringifyTriples(triples)	count = len(triples) print "Sending
#		%s triples to the Triple Store" % (count) print "Example: %.200s" %
#		(triples[15]) print "" time.sleep(6)	os.system('clear')
#		#utils.toTripleStore(stringTriples)
#
#	def parseResults(self, results):
#
#		#Creating metaData attached to the result set requestMeta =
#		self.apikb["service"]["host"] + self.currentRequest["path"] dateMeta
#		= datetime.now().isoformat(' ') metaData = {"request": requestMeta,
#		"date": dateMeta} 
#
#		parsedResults = [] i = 0 for result in results: #Content
#		standardization standardContent = {} for k, v in
#		self.apikb["requests"][self.currentRequest["name"]]["responseMeta"]["standards"].iteritems():
#		if v: try: value = utils.getFromDict(result,v)
#		standardContent.update({k: value}) except Exception as e: continue
#		#Saving results, filtering out posts with empty standard text if
#		self.currentRequest["name"] == "search": if "post.content" not in
#		standardContent.keys(): continue	
#			
#			#Extracting links outLinks =
#			utils.extractLinks(standardContent["post.content"]) if outLinks:
#			standardContent.update({"post.outLinks": outLinks})
#			standardContent.update({"user.platform":
#			self.spiderMeta["platform"], "post.platform":
#			self.spiderMeta["platform"]}) metaData.update({ "counter": i })
#			parsedResults.append({"originalContent": result,
#			"standardContent": \ standardContent, "metaData": metaData}) i
#			+= 1
#
#		return parsedResults def extractLinks(myString): #string extractor
#		links = re.findall("https?://[^ ]+", myString, re.I) return links 
#
#    def makeTriples(parsedResults): triples = [] for parsedResult in
#    parsedResults: #Ids generation userId = "User%s%s" %
#    (parsedResult["standardContent"]["user.platform"],\
        #    parsedResult["standardContent"]["user.id"]) if
        #    parsedResult["standardContent"]["post.id"]: postId =
        #    "Post%s%s" %
        #    (parsedResult["standardContent"]["post.platform"],\
#                    parsedResult["standardContent"]["post.id"])
#                    triples.append([postId, "user", userId])
#
#		#Building triples for k,v in
#		parsedResult["standardContent"].iteritems(): triple = ['','',''] if
#		k.split('.')[0] == "post": triple[0] = postId else: triple[0] =
#		userId triple[1] = k.split('.')[1] triple[2] = v
#		triples.append(triple)
#
#        return triples
#
#def stringifyTriples(triples):
#	stringTriples = [] for triple in triples: stringTriple = [] for item in
#	triple: item = json.dumps(item)
#	stringTriple.append(item.replace('"','')) stringTriple =
#	"\\-".join(stringTriple) stringTriples.append(stringTriple)
#	stringTriples = "\\,".join(stringTriples) return stringTriples
#
#
#JAVA_JAR_PATH = "lib/rdfstore/target/h2rdf-0.1-SNAPSHOT.jar" #MAIN_CLASS =
#"gr.ntua.h2rdf.client.SimplifiedAPI" #COMMAND_TYPE = "put" #SERVER_ADDRESS =
#"ia200124" #DATABASE = "apicrawler-test" #import subprocess #import time
#
#def toTripleStore(stringTriples):
#	subprocess.call(["java", "-cp", JAVA_JAR_PATH, MAIN_CLASS, COMMAND_TYPE,
#	SERVER_ADDRESS, DATABASE, stringTriples])
#
#
#postMappingDict = { "post.id": "url", "user.description": "content",\
#		"post.content": "content", "post.platform": "mimeType",
#		"post.outLinks": "outLinks" } 
#
#userMappingDict = { "user.id": "url", "user.platform": "mimeType",
#"user.name": "content" }
#
#def prepareForHbase(parsedResults):
#	hbaseReadyResults = []	for parsedResult in parsedResults: #Only results
#	with outLinks are sent if "post.outLinks" in
#	parsedResult["standardContent"].keys(): userHbaseReadyResult = {}
#	postHbaseReadyResult = {} for key, value in
#	parsedResult["standardContent"].iteritems(): if key in
#	userMappingDict.keys():
#	userHbaseReadyResult.update({userMappingDict[key]: value}) if key in
#	postMappingDict.keys():
#	postHbaseReadyResult.update({postMappingDict[key]: value})
#	userHbaseReadyResult["mimeType"] = "text/x-User" +
#	userHbaseReadyResult["mimeType"] postHbaseReadyResult["mimeType"] =
#	"text/x-Post" + postHbaseReadyResult["mimeType"] #
#	#hbaseReadyResults.append(userHbaseReadyResult) #
#	hbaseReadyResults.append(postHbaseReadyResult) return hbaseReadyResults
#
## from thrift.transport.TSocket import TSocket ## from
#thrift.transport.TTransport import TBufferedTransport ## from
#thrift.protocol import TBinaryProtocol ## from hbase import Hbase 
#
##TABLE = 'test'
#
## def toHbase(hbaseReadyResults): ##	transport =
#TBufferedTransport(TSocket('localhost', 9090)) ##	transport.open() ##
#protocol = TBinaryProtocol.TBinaryProtocol(transport) ##	client =
#Hbase.Client(protocol) ##	print(client.getTableNames())	##	for
#hbaseReadyResult in hbaseReadyResults: ##		if "outLinks" in
#    hbaseReadyResult.keys(): ##			myMutation = Hbase.Mutation(0,
#        'myColumn', 'myValue') ##			client.mutateRow('test','myRow',
#        myMutation) 
#
#
