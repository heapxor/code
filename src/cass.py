#################
###TODO: 
### 
### insert (TTL for how long we are storing data?)
###
### FIXED:
###    top100 messages per inbox
###    increment 'link number' for column '0'
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
    and keys_cached = 0 and key_cache_save_period = 0
    and column_metadata=[
        {column_name: time, validation_class:AsciiType}, 
        {column_name: size, validation_class:LongType}, 
        {column_name: spam, validation_class:LongType}, 
        {column_name: uid, index_type: KEYS, index_name: uidIdx, validation_class: UTF8Type},
        {column_name: domain, index_type: KEYS, index_name: domainIdx, validation_class: UTF8Type}
        ];

create column family messagesContent with memtable_throughput=64 
    and keys_cached = 0 and key_cache_save_period = 0;
    
create column family messagesAttachment with comparator=UTF8Type and memtable_throughput=64 and 
    keys_cached = 0 and key_cache_save_period = 0 
    and column_metadata=[
        {column_name: 0, validation_class:LongType}, 
        {column_name: 1, validation_class: UTF8Type}
        ];
    
create column family lastInbox with comparator=TimeUUIDType and memtable_throughput=64 and 
    keys_cached = 0 and key_cache_save_period = 0;
 
"""
import StringIO
import logging
import pycassa
from datetime import datetime
from pycassa.cassandra.ttypes import ConsistencyLevel
from pycassa.batch import Mutator
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.index import *
#from pycassa.columnfamily import gm_timestamp
#from eventlet import monkey_patch


#__all__ = ['save_header']
#monkey_patch()

"""

 data inserter and reader for cassandra DB
 
"""

"""
log = pycassa.PycassaLogger()
log.set_logger_name('pycassa_library')
log.set_logger_level('debug')
log.get_logger().addHandler(logging.FileHandler('/home/lenart/src/py.log'))
"""

#tune it!
pool = pycassa.ConnectionPool(keyspace='emailArchive',
server_list=['cvut3:9160', 'cvut4:9160', 'cvut5:9160', 'cvut7:9160', 'cvut8:9160'], 
    prefill=False, pool_size=15, max_overflow=10, max_retries=-1, timeout=5, pool_timeout=200)

#data integrity is our priority on the first place -> QUORUM
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

# write MetaData    
def writeMetaData(key, metaData, statData, attachments):
#metaData: (uid, domain, headerFrom, (subject,code), headerDate, pDate)  #
#statData: (time, size, spam, sender, recipient, envDate) #
    
    subject, code = metaData[3]
    
    #FIX: if field is empty - doesnt create column for it...?
    batch.insert(messagesMetaData, key, { 'uid': metaData[0],
                                          'domain': metaData[1],
                                          'from': metaData[2],
                                          'subject': subject, #UNICODE
                                          'scode' : code,
                                          'hDate': metaData[4],                                          
                                          'time': str(statData[0]),
                                          'size': statData[1],
                                          'spam': statData[2],
                                          'sender': statData[3],
                                          'recipient': statData[4],
                                          'eDate': statData[5]
                                        })
    
    if len(attachments) != 0:
        
        batch.insert(messagesMetaData, key, { 'attachments': str(len(attachments)) })
        
        i=0
        for attch in attachments:
            #fill with zeros because of column sorting is UTF-8 and for fetching attachments in AttachHash() 
            fhash = 'a' + str(i).zfill(3)
            fname = 'b' + str(i).zfill(3)
            fsize = 'c' + str(i).zfill(3)
            
            #attachment: (name, size, hash)
            batch.insert(messagesMetaData, key, {fhash: attch[2]})
            batch.insert(messagesMetaData, key, {fname: attch[0]})
            batch.insert(messagesMetaData, key, {fsize: str(attch[1])})

            i = i + 1

    
    #top100msgs
    #####################
    #email time is from email header
    #date is output of email.utils.parsedate_tz 
    date = metaData[5]
    
    if date != None:    
        time = datetime(date[0],
    		   	         date[1],
		                 date[2],
			             date[3],
			             date[4],
			             date[5]
                        )
    
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
def chunkWriter(key, data, cf, i):    
    #chunk size 512KB (best performance of DB when storing binary data)
    chunkSize = 524288
    
    for chunk in splitter(data, chunkSize):
        id = str(i)
        
        batch.insert(cf, key, { id : chunk })
        batch.send()
        
        i += 1
    
#
# write envelope, header and body into DB
def writeContent(key, envelope, header, body):
    
    messagesContent.insert(key, {'0': envelope})
    messagesContent.insert(key, {'1': header})
    
    """
    Save raw body
    if body size > 1MB then write in chunks
    """
    bodySize = len(body) / 1024
    
    if bodySize > 1024:
        chunkWriter(key, body, messagesContent, 2)
    else:
        messagesContent.insert(key, {'2': body})
        
#
# write attachment data in chunks
# do de-duplication if data actually exist in DB    
def writeAttachment(mHash, fname, data):    
    #KB size
    dataSize = len(data) / 1024    

    #print mHash
    stat = 0
    try:
        messagesAttachment.get(mHash, column_count=1)
    #attachment is not in DB (store it for the first time
    except NotFoundException:
        #>1MB==1024KB
        stat = 1

        messagesAttachment.insert(mHash, {'0': '1'})
        messagesAttachment.insert(mHash, {'1': fname})

        if dataSize > 1024:
            chunkWriter(mHash, data, messagesAttachment, 2)
        else:
            messagesAttachment.insert(mHash, {'2': data})
    
    if stat == 0:
        
        link = messagesAttachment.get(mHash, columns=['0'])
        #overflow?
        link = str(long(link['0']) + 1)        
        
        messagesAttachment.insert(mHash, {'0': link})
        
        print 'INFO: [deduplication in effect]'
            

################
# READING APIs
# 

#
# get #emails from inbox (preferably all of them) 
def getInbox(inbox, emailCount):
    
    inbox_expr = create_index_expression('uid', inbox)
    clause = create_index_clause([inbox_expr,], count=emailCount)
    
    data = messagesMetaData.get_indexed_slices(clause, column_count=emailCount)
    
    return data

#
# get last #emails from inbox sorted by their time (Date field from header)
def getTop(key, emailCount):
    
    try:
        data = lastInbox.get(key, column_count=emailCount, column_start='' )
    except NotFoundException:
        data = None
    
    return data


# test if email exist in DB
# ret:
#     1 - exists, 0 - doesnt exist
def emailCheck(key):
    
    stat = 1
    
    try:
        messagesMetaData.get(key, column_count=1)        
    except NotFoundException:
        stat = 0
        
    return stat
        
# get short email info (for email client)
def getEmailInfo(key):
    
    ret = messagesMetaData.get(key, columns= [
                                             'from', 
                                             'subject',
                                             'hDate',
                                             'size',
					                         'eDate'                                             
                                             ])
    
    try:
        attchs = messagesMetaData.get(key, columns=['attachments'])
        attchs = attchs['attachments']
    except NotFoundException:
        attchs = '0'
        
    ret['attachments'] = attchs
    
    
    return ret

# return: email raw header
def getRawHeader(key):
        
    ret= messagesContent.get(key, columns= ['1'])
    
    return ret['1']

# return: #attachments of email 
def getMimeInfo(key):
    
    try:
        attch = messagesMetaData.get(key, columns= ['attachments'])
        attch = int(attch['attachments'])
    except NotFoundException:
        attch = 0
        
        
    return attch

# return: email raw body (if size of body >1MB then it is stored in chunks) 
def getRawBody(key):
    
    columnsTotal =  messagesContent.get_count(key)
    parts = columnsTotal - 2
    
    body = messagesContent.get(key, column_count = parts, column_start='2', column_finish='')
    
    nBody = []
    
    #join chunks
    for part in range(2, columnsTotal):
        nBody.append(body[str(part)])
    
    
    body = ''.join(nBody)
    
    
    return body

# return: list of attachments with their hashes
def getAttachHash(key, numbAttchs):

    endColumn = 'a' + str(numbAttchs-1).zfill(3)
    
    attchs = messagesMetaData.get(key, column_start='a000', column_finish=endColumn, column_count=numbAttchs)
   
    attchHashList =[]
    
    for k, attch in attchs.iteritems():
        attchHashList.append('DEDUPLICATION:' + attch)
    
    
    return attchHashList 

#
# return: data of attachment (string)    
def getAttachData(key):
    
    columnsTotal = messagesAttachment.get_count(key)
    parts = columnsTotal - 2
   
    #att= {}
    att = messagesAttachment.get(key, column_count=parts, column_start='2', column_finish='')
   
    #print att 
    nAtt = []
    #join chunks of attachment data
    for part in range(2, columnsTotal):
        nAtt.append(att[str(part)])

    data = ''.join(nAtt)

    return data

#
# Create raw email
# 
# return: raw email (string)
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
