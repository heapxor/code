#################
###TODO: 
### Attachment 0 column - number of links
### SHA1 theory 
### insert (TTL for how long we are storing data?)
###
### FIXED:
###    body/attachment in chunks (512KB)
###    inserted into C*
###    first attch hash lookup -> then insert

"""

Cassandra keyspace & CF init
  

bin/cassandra-cli -host cvut3 --batch < schema.txt

schema.txt >
create keyspace emailArchive with
        placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and
        replication_factor = 3;

use emailArchive;

create column family messagesMetaData with comparator=UTF8Type and memtable_throughput=64 
    and keys_cached = 0 and key_cache_save_period = 0;
create column family messagesContent with memtable_throughput=64 
    and keys_cached = 0 and key_cache_save_period = 0;
create column family messagesAttachment with memtable_throughput=64 and 
    keys_cached = 0 and key_cache_save_period = 0;
create column family lastInbox with comparator=TimeUUIDType and memtable_throughput=64 and 
    keys_cached = 0 and key_cache_save_period = 0;

update column family  messagesMetaData with column_metadata=[{column_name: uid, index_type: KEYS, index_name: uidIdx, validation_class: UTF8Type},
 {column_name: domain, index_type: KEYS, index_name: domainIdx, validation_class: UTF8Type}];
 
 
"""

import datetime, StringIO
import logging
import pycassa

from datetime import datetime
from pycassa.cassandra.ttypes import ConsistencyLevel
from pycassa.batch import Mutator
from pycassa.cassandra.ttypes import NotFoundException
#from pycassa.columnfamily import gm_timestamp
from eventlet import monkey_patch


#__all__ = ['save_header']

"""
 data inserter and reader from cassandra
"""

monkey_patch()

"""
log = pycassa.PycassaLogger()
log.set_logger_name('pycassa_library')
log.set_logger_level('debug')
log.get_logger().addHandler(logging.FileHandler('/home/lenart/src/py.log'))
"""


pool = pycassa.ConnectionPool(keyspace='emailArchive',
server_list=['cvut3:9160', 'cvut4:9160', 'cvut5:9160', 'cvut7:9160', 'cvut8:9160'], prefill=False, pool_size=15, 
    max_overflow=10, max_retries=-1, timeout=5, pool_timeout=200)

batch = Mutator(pool, queue_size=50, write_consistency_level=ConsistencyLevel.QUORUM)

messagesMetaData = pycassa.ColumnFamily(pool, 'messagesMetaData',
write_consistency_level=ConsistencyLevel.QUORUM, read_consistency_level=ConsistencyLevel.QUORUM)

messagesContent = pycassa.ColumnFamily(pool, 'messagesContent',
write_consistency_level=ConsistencyLevel.QUORUM, read_consistency_level=ConsistencyLevel.QUORUM)

messagesAttachment = pycassa.ColumnFamily(pool, 'messagesAttachment',
write_consistency_level=ConsistencyLevel.QUORUM, read_consistency_level=ConsistencyLevel.QUORUM)

lastInbox = pycassa.ColumnFamily(pool, 'lastInbox',
write_consistency_level=ConsistencyLevel.QUORUM, read_consistency_level=ConsistencyLevel.QUORUM)


#################
# INSERTING APIs
#
def writeMetaData(key, envelope, header, size, metaData, attachments):  
#metadata (uid, domain, eFrom, subject, date)
#(name, size, hash)
        
    batch.insert(messagesMetaData, key, { 'uid': metaData[0],
                                          'domain': metaData[1],
                                          'envelope': envelope,
                                          'header': header,
                                          'from': metaData[2],
                                          'subject': metaData[3], #??? FIX
                                          'date': metaData[4],
                                          'size': str(size),
                                          'attachments': str(len(attachments))
                                        })
    
    if len(attachments) != 0:
        i=0
        for attch in attachments:
            #fill with zeros because of column sorting (UTF8) and for fetching attachments in AttachHash() 
            fhash = 'a' + str(i).zfill(3)
            fname = 'b' + str(i).zfill(3)
            fsize = 'c' + str(i).zfill(3)
    
            batch.insert(messagesMetaData, key, {fhash: str(attch[2])})
            batch.insert(messagesMetaData, key, {fname: str(attch[0])})
            batch.insert(messagesMetaData, key, {fsize: str(attch[1])})

            i = i + 1


    #print messagesMetaData.get(key)
    #top100msgs
    #####################
    #get it from msg date
    time = datetime.utcnow()    
    batch.insert(lastInbox, metaData[0], {time: key})
        
    batch.send()

#
# split the data l with chunk size n
def splitter(l, n):
    i = 0
    chunk = l[:n]
    while chunk:
        yield chunk
        i += n
        chunk = l[i:i+n]
#
# write chunks into DB
def chunkWriter(key, data, cf):    
    #chunk size 512KB
    chunkSize = 524288
    
    i = 0
    
    for chunk in splitter(data, chunkSize):
        id = str(i)
        batch.insert(cf, key, { id : chunk })
        i += 1

    batch.send()
    
#
# write body into DB
def writeContent(key, body):
    """
    Save raw body
    if body size > 1MB do bulk write
    """
    
    bodySize = len(body) / 1024
    
    if bodySize >  1024:
        chunkWriter(key, body, messagesContent)
    else:
        messagesContent.insert(key, {'0': body})
#
# write attachment data in chunks
# do de-duplication if data actually exist in DB    
def writeAttachment(mHash, data):    
    #KB size
    dataSize = len(data) / 1024    

    stat = 0
    try:
        messagesAttachment.get(mHash, column_count=1)
    except NotFoundException:
        #>1MB==1024KB
        stat = 1
        
        if dataSize > 1024:
            chunkWriter(mHash, data, messagesAttachment)
        else:
            messagesAttachment.insert(mHash, {'0': data})
    
    #debug info
    if stat == 0:
        print 'INFO: [deduplication in effect]'
            


################
# READING APIs
# 

# test if email exist in DB
# return 1 / exists, 0 / doesnt exist
def emailCheck(key):
    
    stat = 1
    
    try:
        messagesMetaData.get(key, column_count=1)        
    except NotFoundException:
        stat = 0
        
    return stat
        
# get short email info (for email client)
def getEmailInfo(key):
    
    ret= messagesMetaData.get(key, columns= [
                                             'from', 
                                             'subject',
                                             'date',
                                             'size',
                                             'attachments'
                                             ])
    
    return ret

# return email raw header
def getRawHeader(key):
        
    ret= messagesMetaData.get(key, columns= ['header'])
    
    return ret['header']

# return #attachments of email 
def getMimeInfo(key):
    
    attch = messagesMetaData.get(key, columns= ['attachments'])  
    
    return int(attch['attachments'])

# return email raw body (if size of body >1MB then it is stored in chunks) 
def getRawBody(key):
    
    columnsNumb =  messagesContent.get_count(key)
    
    body = messagesContent.get(key, column_count = columnsNumb)
    
    nBody = []
    
    #join chunks
    for part in range(columnsNumb):
        nBody.append(body[str(part)])

    
    return ''.join(nBody)

# return list of attachments with their hashes
def getAttachHash(key, numbAttchs):

    endColumn = 'a' + str(numbAttchs-1).zfill(3)
    
    attchs = messagesMetaData.get(key, column_start='a000', column_finish=endColumn, column_count=numbAttchs)
   
    attchHashList =[]
    
    
    for k, attch in attchs.iteritems():
        attchHashList.append('DEDUPLICATION:' + attch)
    
    
    return attchHashList 

# return data of attachment    
def getAttachData(key):
    
    parts = messagesAttachment.get_count(key)
   
    #att= {}
    att = messagesAttachment.get(key, column_count=parts)
    
    nAtt = []
    #join chunks of attachment data
    for part in range(parts):
        nAtt.append(att[str(part)])

    data = ''.join(nAtt)

    return data

#
# recreate source raw email
# 
# return raw email
def getMimeBody(key, attch):
   
    rawBody = getRawBody(key)
    
    attchHashes = getAttachHash(key, attch)    
     
    # raw email 'file' (stringIO)
    # there should be DEDUPLICATION:{hash} marks, if email has the attachments
    body = StringIO.StringIO()
    body.write(rawBody)
    body.seek(0)
    
    # replace 'marks' with raw data of attachments
    newBody = []    
    hash = attchHashes[0]
    
    while True:
        line = body.readline()
        
        #FIX: only inside of the right boundary?
        if line.startswith(hash) and attchHashes:            
            att = getAttachData(hash.rsplit(':')[1])
            newBody.append(att)
            attchHashes.pop(0)
            
            if attchHashes:
                hash = attchHashes[0]

        elif len(line) == 0:
            break #EOF
        else:
            newBody.append(line)
         
    body = ''.join(newBody)
    
    return body
