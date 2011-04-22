import pyes
import time

from pyes.connection import connect_thread_local
from pyes import ES



def get_conn():
    iconn = ES(['cvut3.centrum.cz:9200' ], 
               timeout=4, max_retries=30, bulk_size=400)
    
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
        
    def createIndex(self):
     
        #_indexType = 'email'
        status = None
        try:
            #status = self.iconn.status(self._indexName)  
	    self.iconn.status("a")
        except:
            #put the shards/replicas into creation... ?
            #self.iconn.create_index(self._indexName)
            
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
    
        self.iconn.index(data, self._indexName, "email", _id, bulk=True)
        #self.iconn.flush()
    
    def indexEnvelopeData(self, data, _id):    
        
        self.iconn.index(data, self._indexName, "envelope", _id, bulk=True)    
        #self.iconn.flush()
    #    
    def main(self):
        
        print 'ES pow!'
	self.createIndex()
     

if __name__ == '__main__':
    cEs = ESload()
    cEs.main()

