import pyes
import time

from pyes.connection import connect_thread_local
from pyes import ES

from eventlet import monkey_patch

#monkey_patch()


def get_conn():
    #change to thrift
    iconn = ES(['cvut3:9200', 'cvut4:9200', 'cvut5:9200', 'cvut6:9200', 'cvut7:9200', 'cvut8:9200'], 
 timeout=13, max_retries=30, bulk_size=50)
    
    return iconn

"""
curl -XPUT 'http://cvut4.centrum.cz:9200/archive/' -d '{
   "settings" : {
         "number_of_shards" : 4,
         "number_of_replicas" : 2
    }
}'
                                                        
"""
class ESload():
    
    
    def __init__(self):
        self._indexName = 'archive'
        self.iconn = get_conn()
        self.bulkSize = 0

    def createIndex(self):
     
        #_indexType = 'email'
        status = None
        try:
            status = self.iconn.status(self._indexName)  
        except:
            #put the shards/replicas into creation... ?
            #iconn.create_index(_indexName)
            
            #date string or DATE type
            #inbox shouldnt by analyzed -- but bug X-VF-Scanner-Rcpt-To, 'index': 'not_analyzed'
            mappingsEmail = {
                                 
                            u'inbox': {'type': u'string'},
                            u'from':{'type': u'string'},
                            u'subject':{'type': u'string'},
                            u'date':{'type': u'date'},
                            u'messageID':{'type': u'string', 'index': 'not_analyzed'},
                            u'attachments':{'type': u'string'},
                            u'size':{'type': u'long', 'index': 'not_analyzed'},
                            u'body': {'type': u'string'}
                            
            }     
            
            mappingsEnv = {
                       
                     u'sender': {'type': u'string'},
                     u'recipient': {'type': u'string'},
                     u'ip': {'type': 'ip'}
                    
            }                
            
            mappingsSource = {
                            '_source': {'compress': 'true'}
            }
                 
            status = self.iconn.put_mapping("email", {'properties':mappingsEmail}, self._indexName)        
            self.iconn.put_mapping("email", mappingsSource, self._indexName)
             
            self.iconn.put_mapping("envelope", {'properties':mappingsEnv}, self._indexName)           
            self.iconn.put_mapping("envelope", mappingsSource, self._indexName)
            
    
    def indexEmailData(self, data, _id):
    
        self.iconn.index(data, self._indexName, "email", _id)
        #self.iconn.refresh([self._indexName])
    	self.bulkSize += 1

	if self.bulkSize == 350:
	    #self.iconn.refresh([self._indexName])
	    self.bulkSize = 0
	    #print self.bulkSize

    def indexEnvelopeData(self, data, _id):    
        
        self.iconn.index(data, self._indexName, "envelope", _id)    
        #self.iconn.flush()
	#self.iconn.refresh([self._indexName])

    #    
    def main(self):
        
        print 'ES pow!'
        self.createIndex()
        
"""
if __name__ == '__main__':
    main()
"""
