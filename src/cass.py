"""
Cassandra keyspace & CF init
  ??? for production RF == 3

bin/cassandra-cli -host cvut9 --batch < schema.txt

create keyspace emailArchive with
        placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and
replication_factor = 1;

use emailArchive;

create column family messagesMetaData with memtable_throughput=64;
create column family messagesContent with memtable_throughput=64;
create column family messagesAttachment with memtable_throughput=64;
create column family lastInbox with comparator=TimeUUIDType and memtable_throughput=64;

update column family  messagesMetaData with column_metadata=[{column_name: uid, index_type: KEYS, index_name: uidIdx, validation_class: UTF8Type},
 {column_name: domain, index_type: KEYS, index_name: domainIdx, validation_class: UTF8Type}];
"""

import datetime
from datetime import datetime

import pycassa
from pycassa.cassandra.ttypes import ConsistencyLevel
from pycassa.batch import Mutator

__all__ = ['save_header']


pool = pycassa.ConnectionPool(keyspace='emailArchive',
server_list=['cvut3:9160'], prefill=False)

batch = Mutator(pool, queue_size=50)

messagesMetaData = pycassa.ColumnFamily(pool, 'messagesMetaData',
write_consistency_level=ConsistencyLevel.QUORUM)

messagesContent = pycassa.ColumnFamily(pool, 'messagesContent',
write_consistency_level=ConsistencyLevel.QUORUM)

messagesAttachment = pycassa.ColumnFamily(pool, 'messagesAttachment',
write_consistency_level=ConsistencyLevel.QUORUM)

lastInbox = pycassa.ColumnFamily(pool, 'lastInbox',
write_consistency_level=ConsistencyLevel.QUORUM)



# INSERTING APIs

def writeMetaData(key, envelope, header, size, metaData, attachments):  
#metadata (uid, domain, eFrom, subject, date)
#attach ((msg.get_filename(), len(msg.get_payload()), mHash, '--' + boundary)

    batch.insert(messagesMetaData, key, { 'uid': metaData[0],
                                          'domain': metaData[1],
                                          'envelope': envelope,
                                          'header': header,
                                          'from': metaData[2],
                                          'subject': metaData[3], #???? FIX
                                          'date': metaData[4],
                                          'size': str(size),
                                          'attachments': str(len(attachments))
                                        })
    
    if len(attachments) != 0:
        i=0
        for attch in attachments: 
            cname = 'a' + str(i)   
            batch.insert(messagesMetaData, key, { cname: 
                                                         'filename:' + str(attch[0]) + 
                                                          ',size:'+ str(attch[1]) + 
                                                          ',hash:'+ str(attch[2]) + 
                                                          ',boundary:' + str(attch[3])
                                                  })
            i = i + 1

    #top100msgs
    #####################
    #get it from msg date
    time = datetime.utcnow()    
    batch.insert(lastInbox, metaData[0], {time: key})
        
    batch.send()
#
def chunkWriter(key, data):
    
    print 'a'
#    
def writeContent(key, body):
    """
    Save raw body
    if > 1MB do bulk write
    """
    bodySize = len(body) / 1024
    
    if bodySize >  1024:
        chunkWriter(key, body)
    else:
        messagesContent.insert(key, {'1': body})
#    
def writeAttachment(mHash, data):
    
    dataSize = len(data) / 1024    
    
    if dataSize > 1024:
        chunkWriter(mHash, data)
    else:
        messagesAttachment.insert(mHash, {'1': data})
#
#
#
# READING APIs
#
def getHeader(key):
    print key
    col = messagesMetaData.get_count(key);     
    print col
    x = messagesMetaData.get(key, column_count=col)
    return x

