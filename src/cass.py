"""
Cassandra keyspace & CF init
  ??? for production RF == 3

bin/cassandra-cli -host cvut9 --batch < schema.txt

create keyspace emailArchive with
        placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and
replication_factor = 1;

use emailArchive;

create column family messagesMetaData with memtable_throughput=64;
create column family messagesContext with memtable_throughput=64;
create column family messagesAttachment with memtable_throughput=64;
create column family userInbox with memtable_throughput=64;

"""

import pycassa
from pycassa.cassandra.ttypes import ConsistencyLevel

__all__ = ['save_header']



pool = pycassa.ConnectionPool(keyspace='emailArchive',
server_list=['cvut9:9160'], prefill=False)


messagesMetaData = pycassa.ColumnFamily(pool, 'messagesMetaData',
write_consistency_level=ConsistencyLevel.QUORUM)

messagesContext = pycassa.ColumnFamily(pool, 'messagesContext',
write_consistency_level=ConsistencyLevel.QUORUM)

messagesAttachment = pycassa.ColumnFamily(pool, 'messagesAttachment',
write_consistency_level=ConsistencyLevel.QUORUM)

userInbox = pycassa.ColumnFamily(pool, 'userInbox',
write_consistency_level=ConsistencyLevel.QUORUM)


# INSERTING APIs

def writeHeader(key, data):
    """
    Save raw header
    """
    messagesMetaData.insert(key, {'Header': data})

def writeBody(key, data):
    """
    Save raw body
    ??? ale ak je >1MB do bulk write
    """
    messagesContext.insert(key, {'Body': data})
    
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







def getHeader(key):
    header = messagesMetaData.get(key, columns=['Header'])
    return header['Header']
