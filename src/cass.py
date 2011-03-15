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

def insert(key, data, columnName):
    """
    Save raw header
    """
    messagesMetaData.insert(key, {columnName: data})


def save_body(body):
    messagesContext.insert('1', {'test': body})
    
def save_attachment(att):
    messagesAttachment.insert('1', {'test': 'body'})
    

def get_header(key):
    header = messagesMetaData.get(key, columns=['Header'])
    return header['Header']
