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


__all__ = ['save_header']



pool = pycassa.ConnectionPool(keyspace='emailArchive',
server_list=['cvut9:9160'], prefill=False)


messagesMetaData = pycassa.ColumnFamily(pool, 'messagesMetaData',
write_consistency_level=ConsistencyLevel.QUORUM)

messagesContent = pycassa.ColumnFamily(pool, 'messagesContent',
write_consistency_level=ConsistencyLevel.QUORUM)

messagesAttachment = pycassa.ColumnFamily(pool, 'messagesAttachment',
write_consistency_level=ConsistencyLevel.QUORUM)

lastInbox = pycassa.ColumnFamily(pool, 'lastInbox',
write_consistency_level=ConsistencyLevel.QUORUM)



# INSERTING APIs

def writeHeader(key, data):
    """
    Save raw header
    """
    messagesMetaData.insert(key, {'Header': data})
    
def writeUid(key, uid, domain):    
    
    messagesMetaData.insert(key, {'uid': uid, 'domain': domain})
    

def writeBody(key, data):
    """
    Save raw body
    ??? ale ak je >1MB do bulk write
    """
    messagesContent.insert(key, {'Body': data})
    
def writeEnevelope(key, data):
    """
    Save envelope
    """
    messagesMetaData.insert(key, {'Envelope': data})
    
def writeMetaAttachment(key, data):
    """
    Save attachment's metadata
    data: list of att's details
    """
    #messagesMetaData.insert(key, {'Attachments': data})

def writeDataAttachment(key, data):
    """
    key: attachment hash
    data: attachment data
    """
    messagesAttachment.insert(key, {'1': data})



def writeInboxLast(uid, messageId):
    
    time = datetime.utcnow()
    
    lastInbox.insert(uid, {time: messageId})
    

def getHeader(key):
    header = messagesMetaData.get(key, columns=['Header'])
    return header['Header']

