import sys
import email
import time

from emailParser import getMetaData
from emailParser import createKey
from email.errors import NoBoundaryInMultipartDefect
from email.utils import quote
import es
from eventlet import monkey_patch

#monkey_patch()



def elasticEmail(envelope, metaData, attchs, body):
#metaData: (uid, domain, eFrom, subject, date)    
    
    #8 Message-ID
    #4 Raw size of message
    fields = envelope.split('\t')
    messageId = fields[7].strip('<>')
    rawSize = fields[3]

    inbox = metaData[0]
    eFrom = metaData[2]
    #??? encode it somehow
    subject = metaData[3]
    
    #Date from email header has format: Fri, 09 Nov 2001 01:08:47 GMT (RFC2822)
    #-> transform it into "2009-11-15T14:12:12" for fulltext search 
    date = metaData[4]    
    d = time.strptime(date, "%a, %d %b %Y %H:%M:%S %Z")    
    date = str(d.tm_year) + "-" + str(d.tm_mon) + "-" + str(d.tm_mday) + "T" + str(d.tm_hour) + ":" + str(d.tm_min) + ":" + str(d.tm_sec)    
    
    #attachments list
    attchList = "["
         
    i = 0
    for att in attchs:
        attchList +=  '"' + att + '"'
        i += 1
            
        if i < len(attchs):
            attchList += ','
    
    attchList += "]"
    data = dict(inbox=inbox,
                subject=subject,
                date=date,
                messageID=messageId,
                attachments=attchList,
                size=rawSize,
                body=body)
    
    data['from'] = eFrom
    
    return data

#
def getBody(msg):
    
    body = []
    attchs = []
    
    for part in msg.walk():
       
        #fix that , now only explicitly written content type text/plain is passed to ES
        #check for base64 ? to be sure its attch ? --> if none then plain ? 
        missing = object()
        value = part.get('content-type', missing)

        if value is not missing:

            if part.get_content_type() == 'text/plain':
             
                charset = part.get_param('charset')
            
                if charset:
                    charset = quote(charset)
                else:
                    charset = quote('utf-8')

                #create unicode representation of DATA
                try:
                    body.append(part.get_payload().decode(charset, 'ignore'))
                except LookupError:
                    body = part.get_payload().decode('utf-8', 'ignore')

           
        if (part.get_content_type() != 'text/plain' and part.get_content_type() != 'text/html' and
            part.get_content_maintype() != 'multipart' and part.get_content_maintype() != 'message' and 
            part.get_content_type() != 'message/rfc822'):
            
            fileName = part.get_filename(None)
            
            if fileName == None:
                fileName = ''
            else:
                attchs.append(fileName)
            
        
    body = ''.join(body)
    #print body.encode('iso-8859-2',  'ignore' )
    
    return (body, attchs)
#
def elasticEnvelope(envelope):
        
    fields = envelope.split('\t')
    
    #???by qmail-scanner documantation - second field should be qmila-scanner[PID]     
    #5 (envelope sender)
    #6 (envelope recipient)    
    sender = fields[4]
    recipient = fields[5]
    
    #2 IP address.. Clear:RC:1(88.208.65.55):    
    ip = fields[1][fields[1].find('(')+1 : fields[1].find(')')]

    #date from envelope
    #"2009-11-15T14:12:12",
    date = fields[0]
    d = time.strptime(date, "%a, %d %b %Y %H:%M:%S %Z")
    date = str(d.tm_year) + "-" + str(d.tm_mon) + "-" + str(d.tm_mday) + "T" + str(d.tm_hour) + ":" + str(d.tm_min) + ":" + str(d.tm_sec)    
        
        
    data = dict(sender=sender, 
                recipient=recipient,
                ip=ip,
                date=date)
    
    return data

#
def mimeEmail(msg, envelope, metaData, ff):
    
    (body, attchs) = getBody(msg)
    
    data = elasticEmail(envelope, metaData, attchs, body)
    
    """
    if len(body) > 100000:
        print 'SIZE!!' + str(ff)
    """
    
    return data

def rawEmail(msg, envelope, metaData):
    
    charset = msg.get_param('charset')
    
    if charset:
        charset = quote(charset)
    else:
        charset = quote('utf-8')
    
    try:
        body = msg.get_payload().decode(charset, 'ignore')
    except LookupError:
        body = msg.get_payload().decode('utf-8', 'ignore')
        
    """
    #print 'BODY:' + str(len(body))
    if len(body) > 100000:
       print 'SIZE!!'
    """
    data = elasticEmail(envelope, metaData, [], body)
    
    return data

##############################################################################


def parseEmail(emailFile):

    escon = es.ESload()

    start = time.time()

    f = open(emailFile, 'r')
    msg = email.message_from_file(f)
    f.close()

    env = open(emailFile + '.envelope', 'r')
    envelope = env.readline()
    env.close()
    
    metaData = getMetaData(msg)
    
    try:  
        if msg.is_multipart():
            data = mimeEmail(msg, envelope, metaData, )
        else:
            data = rawEmail(msg, envelope, metaData)
        
    except NoBoundaryInMultipartDefect:
        data = rawEmail(msg, envelope, metaData, )
    
    
    key = createKey(metaData[0], envelope)
    envData = elasticEnvelope(envelope)

    #write data into ES
    escon.indexEmailData(data, key)
    escon.indexEnvelopeData(envData, key)
     
    duration = time.time() - start 
   
    return duration

#
def main():
    
    email = sys.argv[1]
    
    start = time.time()
    
    parseEmail(email,)
        
    duration = time.time() - start

    print duration

#
if __name__ == '__main__':
    
    main()
