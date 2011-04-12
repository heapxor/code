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

import datetime, StringIO
from datetime import datetime

import pycassa
from pycassa.cassandra.ttypes import ConsistencyLevel
from pycassa.batch import Mutator
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.columnfamily import gm_timestamp

#__all__ = ['save_header']


pool = pycassa.ConnectionPool(keyspace='emailArchive',
server_list=['cvut3:9160', 'cvut4:9160'], prefill=False)

batch = Mutator(pool, queue_size=100)

messagesMetaData = pycassa.ColumnFamily(pool, 'messagesMetaData',
write_consistency_level=ConsistencyLevel.QUORUM)

messagesContent = pycassa.ColumnFamily(pool, 'messagesContent',
write_consistency_level=ConsistencyLevel.QUORUM)

messagesAttachment = pycassa.ColumnFamily(pool, 'messagesAttachment',
write_consistency_level=ConsistencyLevel.QUORUM)

lastInbox = pycassa.ColumnFamily(pool, 'lastInbox',
write_consistency_level=ConsistencyLevel.QUORUM)


#################
###TODO: 
### Attachment 0 column - number of links
### SHA1 ?? theory 
###
### FIXED:
###    body/attachment in chunks (512KB)
###    inserted into C*
###    first attch hash lookup -> then inser


#################
# INSERTING APIs

def writeMetaData(key, envelope, header, size, metaData, attachments):  
#metadata (uid, domain, eFrom, subject, date)
#FIX:(name, size, hash) json?

#    print 'inserting'
        
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
    
    #print attachments
    #print attachments 
    if len(attachments) != 0:
        i=0
        for attch in attachments:
	    #fill with zeros because of column sorting and fetching attachments in AttachHash() 
            fhash = 'a' + str(i).zfill(3)
	    fname = 'b' + str(i).zfill(3)
	    fsize = 'c' + str(i).zfill(3)

	    batch.insert(messagesMetaData, key, {fhash: str(attch[2])})
	    batch.insert(messagesMetaData, key, {fname: str(attch[0])})
	    batch.insert(messagesMetaData, key, {fsize: str(attch[1])})

	    """
            batch.insert(messagesMetaData, key, { cname: 
                                                               str(attch[0]) + 
                                                          ','+ str(attch[1]) + 
                                                          ','+ str(attch[2])
                                                  })
	    """

            i = i + 1

    #print messagesMetaData.get(key)
    #top100msgs
    #####################
    #get it from msg date
    time = datetime.utcnow()    
    batch.insert(lastInbox, metaData[0], {time: key})
        
    batch.send()
#

def splitter(l, n):
    i = 0
    chunk = l[:n]
    while chunk:
        yield chunk
        i += n
        chunk = l[i:i+n]
#
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
def writeContent(key, body):
    """
    Save raw body
    if > 1MB do bulk write
    """
    bodySize = len(body) / 1024
    
    if bodySize >  1024:
        chunkWriter(key, body, messagesContent)
    else:
        messagesContent.insert(key, {'0': body})
#    
def writeAttachment(mHash, data):    
    #KB
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
            
    if stat == 0:
        print 'AttachmentWriter:[deduplication in effect]'
            
    
################
#
# READING APIs
#
def getHeader(key):
    #print key
    col = messagesMetaData.get_count(key);     
    #print col
    x = messagesMetaData.get(key, column_count=col)
    return x

#
def getEmailInfo(key):
    
    ret= messagesMetaData.get(key, columns= [
                                             'from', 
                                             'subject',
                                             'date',
                                             'size',
                                             'attachments'
                                             ])
    
    return ret


def getRawHeader(key):
        
    ret= messagesMetaData.get(key, columns= ['header'])
    
    return ret['header']

def getMimeInfo(key):
    
    attch = messagesMetaData.get(key, columns= ['attachments'])  
    
    return int(attch['attachments'])
    
def getRawBody(key):
    
    columnsNumb =  messagesContent.get_count(key)
    
    body = messagesContent.get(key, column_count = columnsNumb)
    
    nBody = []
    #join chunks
    for k, bodyPart in body.iteritems():
        nBody.append(bodyPart)
    
    
    return ''.join(nBody)

def getAttachHash(key, attch):

    #pocet stlpcov - uz ich len potom fetchuj a spajaj...
    #get_count()
    #multiget_count
    #print attch 
    eColumn = 'a' + str(attch-1).zfill(3)
    attch = messagesMetaData.get(key, column_start='a000', column_finish=eColumn)
   
    #print messagesMetaData.get(key)
    
    #print attch
    
    attchHashList =[]
    for k, att in attch.iteritems():
        #attchHashList.append('DEDUPLICATION:' + att.rsplit(',')[2])
	attchHashList.append('DEDUPLICATION:' + att)

    return attchHashList 
    
def getAttachData(key):
    
    parts = messagesAttachment.get_count(key)
   
    att= {}
    att = messagesAttachment.get(key, column_count=parts)
    

    nAtt = []
    #join chunks
    for part in range(parts):
	nAtt.append(att[str(part)])

    return ''.join(nAtt)
#
def getMimeBody(key, attch):
    
    rawBody = getRawBody(key)
    
    attchHashes = getAttachHash(key, attch)    
    

    #print attchHashes

    body = StringIO.StringIO()
    body.write(rawBody)
    body.seek(0)
    
    sig = 0
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
    
    return ''.join(newBody)

