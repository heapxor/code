import pyes
import time

from pyes.connection import connect_thread_local
from pyes import ES



#change to thrift
iconn = ES(['cvut3.centrum.cz:9200', 'cvut4.centrum.cz:9200', 'cvut5.centrum.cz:9200', 'cvut6.centrum.cz:9200'], 
           timeout=4, max_retries=30, bulk_size=400)
_indexName = 'archive'

"""
curl -XPUT 'http://cvut4.centrum.cz:9200/archive/' -d '{
   "settings" : {
         "number_of_shards" : 4,
         "number_of_replicas" : 2
    }
}'
                                                        
"""

def createIndex():
 
    #_indexType = 'email'
    status = None
    try:
        status = iconn.status(_indexName)  
    except:
        #put the shards/replicas into creation... ?
        iconn.create_index(_indexName)
        
        #date string or DATE type?
        mappingsEmail = {
                             
                        u'inbox': {'type': u'string', 'index': 'not_analyzed'},
                        u'from':{'type': u'string'},
                        u'subject':{'type': u'string'},
                        u'date':{'type': u'string'},
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
             
        status = iconn.put_mapping("email", {'properties':mappingsEmail}, _indexName)        
        iconn.put_mapping("email", mappingsSource, _indexName)
         
        iconn.put_mapping("envelope", {'properties':mappingsEnv}, _indexName)           
        iconn.put_mapping("envelope", mappingsSource, _indexName)
        

def indexEmailData(data, _id):

    iconn.index(data, _indexName, "email", _id)

def indexEnvelopeData(data, _id):    
    
    iconn.index(data, _indexName, "envelope", _id)    
    
#    
def main():
    
    print 'ES pow!'
    createIndex()
    

if __name__ == '__main__':
    main()