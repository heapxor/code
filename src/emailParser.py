#!/usr/bin/python

##################################
## TODO:
##     folding in get_content_type
##     data expiration (?)
##     write attachments data after check that email is not bad
##     
##
## FIXED:
##    more sophisticated email KEY
##    windows/unix newline
##    newlines in txt attachment fixed
##    mime body headers with FOLDING 
##    message/rfc822
##    Windows OS newlines -> put it back (BufferedData removes it)


import sys
import email
#import cass
import os
import time
import hashlib
#import StringIO
from email.errors import NoBoundaryInMultipartDefect
from email.utils import parseaddr
from email.header import decode_header
from email.utils import quote
#from email.Iterators import _structure
#from email.iterators import body_line_iterator
#from email.utils import parseaddr
#from email.utils import getaddresses


class BufferedData():
    
    def __init__(self, f):            
            self.lines = []            
            self.newlineStack = []
            self.f = f
            
    def readline(self):        
        if not self.lines:
            line = self.f.readline()
            #line = line.replace('\r\n', '\n').replace('\r', '\n')
       
        else:
            line = self.lines.pop(0)
        
        return line
    
    def fakeNewLine(self, newline):        
        self.newlineStack.append(newline)

    
    def unreadline(self, line):
        self.lines.append(line)

###############################################################################


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
    
    #Date from email header - analyze it?
    #date = metaData[4]
    
    #date from envelope
    #"2009-11-15T14:12:12",
    date = fields[0]
    d = time.strptime(date, "%a, %d %b %Y %H:%M:%S %Z")
    date = str(d.tm_year) + "-" + str(d.tm_mon) + "-" + str(d.tm_mday) + "T" + str(d.tm_hour) + ":" + str(d.tm_min) + ":" + str(d.tm_sec)    
    
    
    
    #attachments list
    attchList = "["
    i = 0
    for att in attchs:
        attchList +=  '"' + att[2] + '"'
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

def elasticEnvelope(envelope):
        
    fields = envelope.split('\t')
    
    #???by qmail-scanner documantation - second field should be qmila-scanner[PID]     
    #5 (envelope sender)
    #6 (envelope recipient)    
    sender = fields[4]
    recipient = fields[5]
    
    #2 IP address.. Clear:RC:1(88.208.65.55):    
    ip = fields[1][fields[1].find('(')+1 : fields[1].find(')')]
    
    
    data = dict(sender=sender, 
                recipient=recipient,
                ip=ip)
    
    return data

#
def createKey(uid, envelope):
    
    fields = envelope.split('\t') 
    messageId = fields[7].strip('<>')
    
    #date + time when email was processed by SMTPD server
    date = fields[0]
    d = time.strptime(date, "%a, %d %b %Y %H:%M:%S %Z")

    #YearMonthDayHourMinSec
    date = str(d.tm_year) + str(d.tm_mon) + str(d.tm_mday) + str(d.tm_hour) + str(d.tm_min) + str(d.tm_sec)    
    data = uid + messageId
    
    m = hashlib.sha1()      
    m.update(data)
    
    key = m.hexdigest() + date
        
    
    return key

def rawHeader(msg):
    header = []
    
    while True:
        line = msg.readline()
      
        header.append(line)
      
        if line == '\n' or line == '\r\n':
            break
 
        #because the body is optional
        if len(line) == 0:
            break #EOF
    
    header = ''.join(header)
    
    return header
#
def rawBody(email):    
    body = []
    
    while True:
        line = email.readline()
        
        body.append(line)
        
        if len(line) == 0:
            break #EOF
    
    body = ''.join(body)
    
    return body
#    
def getMetaData(msg):    
    #the real recipient is in the email header, because of tapping
    #X-VF-Scanner-Rcpt-To: x@y
    #TODO:??? normally its in envelope
    uid = msg.get('X-VF-Scanner-Rcpt-To')
    
    if uid == None:
        uid = 'charvat@cvut1.centrum.cz'
    
    #TODO:??? whats the domain format, fix for fulltext? 
    domain = uid.partition('@')[2]

    #From field in email header is mandatory by RFC2822
    eFrom = msg.get('From')
    
    if eFrom == None:
        eFrom = ''
    else: 
        eFrom = parseaddr(eFrom)[1]
        
    #Date field in email header is mandatory by RFC2822
    #format: Fri, 18 Mar 2011 16:30:00 +0000
    #??? from email lib use parsedate
    date = msg.get('Date')
    if date == None:
        date = ''
        
    subject = msg.get('Subject')
    if subject == None:
        subject = '' 
    else:
        subject = decode_header(subject)
        usubject = ''
        
        for part in subject:

            data, charSet = part

            if charSet == None:
                data = data.decode('utf-8', 'ignore')
            else:
                data = data.decode(charSet)
                
                usubject += data 
    
    return (uid, domain, eFrom, usubject, date)
#    
def metaAttachment(msg, parentType, boundary, attach ,bSet):
        
    if msg.is_multipart():
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()
            bSet.add('--' + boundary)
            bSet.add('--' + boundary + '--')
        
        parentType = msg.get_content_type()
        
        for subpart in msg.get_payload():
            metaAttachment(subpart, parentType, boundary, attach, bSet)
    
    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart' and msg.get_content_maintype() != 'message' and 
            msg.get_content_type() != 'message/rfc822'):
                
        boundary = '--' + boundary
              
        try:                            
            fileName = str(msg.get_filename())                
        except UnicodeEncodeError:                
            fileName = msg.get_filename().encode('utf8')
        
        
        if parentType.lower() == 'message/rfc822':
            attach.append((boundary, parentType, fileName))
        else:
            attach.append((boundary, msg.get_content_type(), fileName))        
#        
def writeAttachment(data):
    
    #??? for sure sha1
    m = hashlib.sha1()      
    m.update(data)
    key = m.hexdigest()
    
    ###cass.writeAttachment(key, data)                                 
    
    return key 
#
def newRawBody(key, f, attachments, bSet):    
   
    stat = 6;

    buff = BufferedData(f)    
    data = []
    body = []
    bound = ()
    attchList = []
    boundaryHeader = 0
    attchTotal = len(attachments)
    attchWritten = 0
    
    while True:
        
        line = buff.readline()
        
        if len(line) == 0:
            #print 'EOF' 
            break        
     
        #automata start                        
        if stat == 6:
            #print 'Stat:6'
            body.append(line)
            
            #if attchWritten > 2 and bound[2] == '08022011222.jpg':
                #print '>>>>' + line,
                
            if attachments and line[0:len(line) - 1] == attachments[0][0]:                        
                #possible attach boundary        
                #attachments.pop(0)      
                #print line      
                bound = attachments[0]                
                stat = 1
                                  
        elif stat == 1:
            #print "Stat:1"
            
            body.append(line)                           
            #is there content-type? / some emails use diff *case of content-type
            if 'content-type:' in line.lower():              
                if bound[1] in line.lower():         
                    boundaryHeader = 1
                else:
                    #possible folding in header field
                    
                    line = buff.readline()
                    #print repr(line[0]),
                    if line[0] == '\t' or line[0] == ' ':
                        body.append(line)
                                
                        if bound[1] in line.lower():
                            boundaryHeader = 1                                     
                    else:                        
                        buff.unreadline(line)
            elif line == '\n' or line == '\r\n' or line == '\r':
                if boundaryHeader == 1:
                    
                    if bound[1].lower() == 'message/rfc822':
                    
                        stat = 5
                    else:
                        stat = 2
                else:
                    stat = 6      
                    
        elif stat == 2:   
            #print "Stat:2" 
            boundaryHeader = 0
            #???
            if line == '\n' or line == '\r\n' or line == '\r':
                #print repr(line)
                buff.fakeNewLine(line)
                #print buff.newlineStack
                stat = 3                
            elif line[0:len(line)-1] in bSet:
                buff.unreadline(line)
                stat = 4                
            else:
                data.append(line)
                
            prevStat = 2           
                       
        elif stat == 3:
            #print "Stat:3"          

            if line == '\n' or line == '\r\n' or line == '\r':
                buff.fakeNewLine(line)
                #print buff.newlineStack
            elif line[0:len(line)-1] in bSet:
                buff.unreadline(line)
                stat = 4
                
            else: #text  
                stat = 2
                         
            if stat == 2 or stat == 4:        
                #print 'cistim stack:' + str(len(buff.newlineStack))
                body2 = []
                for newLine in buff.newlineStack:
                    if stat == 4:                    
                        body2.append(newLine)                                        
                    else:
                        data.append(newLine)
                    
                buff.newlineStack = []        
                if stat == 2:
                    data.append(line)
            
            prevStat = 3
            
        elif stat == 4:
            #print "Stat:4"          
            #ddata = ''.join(data)  
            attKey = writeAttachment(''.join(data))                
            hash = 'DEDUPLICATION:' + attKey + '\n'
            
            body.append(hash)
            
            #create list of attachmentsData -
            
            #body.append(''.join(data))
                
            #(name, size, hash), its metadata for messageMetaData CF
            #metaData  = (bound[2], len(ddata), attKey)
            metaData  = (bound[2], len(''.join(data)), attKey)
            attchList.append(metaData)
            
            data = []
            
            if prevStat == 3:
                for newLines in body2:
                    body.append(newLines)
                                                
            stat = 6
            
            #print 'att written:' + bound[2]

            
            attchWritten += 1
            attachments.pop(0)
            
            buff.unreadline(line)
            
            
        elif stat == 5:
            #print line
            #print 'Stat:5'
            
            body.append(line)
            #???
            if line == '\n' or line == '\r\n' or line == '\r':
                stat = 2
     
                            
    body = ''.join(body)                  
                            
    if attchTotal != attchWritten:
        #print 'bad email'
        return ()
                            
    return (body, attchList)      

#
def mimeEmail(f, msg, envelope, size):
         
    header = rawHeader(f)    
    metaData = getMetaData(msg)  
    
    #metaData[0] is uid (inbox)
    key = createKey(metaData[0], envelope)

    #find attachment's boundary and write attachments
    attachments = []
    bSet = set()

    metaAttachment(msg, "", 0, attachments, bSet)

    
    if len(attachments) != 0:
        aES = []
        
        for a in attachments:
            aES.append(a)
        
        ret = newRawBody(key, f, attachments, bSet)
        
        if ret:
            (body, attach) = ret
            
            envData = elasticEnvelope(envelope)
            emailData = elasticEmail(envelope, metaData, aES, body)
            
            #print envData,
            #print '>>>'
            #print emailData,
            """
            cass.writeMetaData(key, envelope, header, size, metaData, attach)    
            cass.writeContent(key, body)
            """            
            es.indexEmailData(emailData, key)
            es.indexEnvelopeData(envData, key)
            
            
        else:
            print 'Error: Bad email <' + key + '>'
    #MIME email w/out attachments
    else:        
        body = rawBody(f)
        
        envData = elasticEnvelope(envelope)
        emailData = elasticEmail(envelope, metaData, attachments, body)
        
        """
        cass.writeMetaData(key, envelope, header, size, metaData, [])    
        cass.writeContent(key, body)        
        """
        es.indexEmailData(emailData, key)
        es.indexEnvelopeData(envData, key)
        
#        
def rawEmail(f, msg, envelope, size):
    
    header = rawHeader(f)
    body = rawBody(f)
    metaData = getMetaData(msg)        
    
    #metaData[0] is uid (inbox)
    key = createKey(metaData[0], envelope)

    emailData = elasticEmail(envelope, metaData, [], body)
    envData = elasticEnvelope(envelope)
    
    """
    cass.writeMetaData(key, envelope, header, size, metaData, [])
    cass.writeContent(key, body)
    """
    es.indexEmailData(emailData, key)
    es.indexEnvelopeData(envData, key)
    
    
##############################################################################


def parseEmail(emailFile):
    
    start = time.time()

    f = open(emailFile, 'r')
    msg = email.message_from_file(f)
    f.seek(0)

    env = open(emailFile + '.envelope', 'r')
    envelope = env.readline()
    env.close()

    #get from Envelope
    size = os.path.getsize(emailFile)

    try:  
        if msg.is_multipart():
            mimeEmail(f, msg, envelope, size)
        else:
            rawEmail(f, msg, envelope, size)
        
    except NoBoundaryInMultipartDefect:
        rawEmail(f, msg, envelope, size)
    

    f.close()

    duration = time.time() - start 
   
    return duration

#
def main():
    
    email = sys.argv[1]
    
    start = time.time()
    
    parseEmail(email)
        
    duration = time.time() - start

    print duration

#
if __name__ == '__main__':
    
    main()
