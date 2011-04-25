#!/usr/bin/python

##################################
## TODO:
##     data expiration
##     folding in get_content_type
##     write attachments data after check that email is not bad / unimportant...
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
import cass
import time
import hashlib
from email.errors import NoBoundaryInMultipartDefect
from email.utils import parseaddr
from email.header import decode_header
from email.utils import mktime_tz
from email.utils import formatdate
from email.utils import parsedate_tz
from time import strptime
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
        else:
            line = self.lines.pop(0)
        
        return line
    
    def fakeNewLine(self, newline):        
        self.newlineStack.append(newline)

    
    def unreadline(self, line):
        self.lines.append(line)

###############################################################################
#
#statistical data (for MAPREDUCE)
#
#   ret:
#       time, size, spam, sender, recipient, envDate #
def getStatsData(envelope):
    
    fields = envelope.split('\t')
    
    time = fields[2]
    size = fields[3]
    
    #by qmail-scanner documantation - second field should be qmila-scanner[PID]     
    #5 (envelope sender)
    #6 (envelope recipient)    
    sender = fields[4]
    recipient = fields[5]
    
    idx = fields[1].find('SA:')
    
    if idx == -1:
        spamFlag = '0' 
    else:
        spamFlag = fields[1][idx+3]
    
    #date from envelope
    #"2009-11-15T14:12:12",
    date = fields[0]    
    d = strptime(date, "%a, %d %b %Y %H:%M:%S %Z")
    date = str(d.tm_year) + "-" + str(d.tm_mon) + "-" + str(d.tm_mday) + "T" + str(d.tm_hour) + ":" + str(d.tm_min) + ":" + str(d.tm_sec)    
    
    
    return (time, size, spamFlag, sender, recipient, date)
    

#
# key = sha256(uid + messageId + date)
# uid is inbox name (jan@mak.com)
def createKey(uid, envelope):
    
    fields = envelope.split('\t') 
    messageId = fields[7].strip('<>')
    
    #date + time when email was processed by SMTPD server
    date = fields[0]
    d = time.strptime(date, "%a, %d %b %Y %H:%M:%S %Z")

    #YearMonthDayHourMinSec
    date = str(d.tm_year) + str(d.tm_mon) + str(d.tm_mday) + str(d.tm_hour) + str(d.tm_min) + str(d.tm_sec)    
    data = uid + messageId + date
    
    m = hashlib.sha256()      
    m.update(data)
    
    key = m.hexdigest()
        
    
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
# ret:
#     uid, (jan@mak.com)
#     domain name from uid  (mak.com)
#     from, subject, date fields from email HEADER
#
# - used by esParse
def getMetaData(msg):    
    
    #TODO: normally its in envelope
    #the real recipient is in the email header, because of tapping
    #X-VF-Scanner-Rcpt-To: x@y
    #---- qmail configuration BUGS -> have to solve charvat
    #     - this field is missing
    #     - this field has more then one recipient!!! 
    uid = msg.get('X-VF-Scanner-Rcpt-To')
    
    #because of ...
    if uid == None:
        uid = 'charvat@cvut1.centrum.cz'
    else:
        idx = uid.find(',')
    
        if idx != -1:
            uid = uid[:idx]
     
    
    domain = uid.partition('@')[2]

    #'From' field in email header is mandatory by RFC2822
    headerFrom = msg.get('From')
    
    if headerFrom == None:
        headerFrom = ''
    else: 
        headerFrom = parseaddr(headerFrom)[1]
        
    #Date field in email header is mandatory by RFC2822
    #format: Fri, 18 Mar 2011 16:30:00 +0000 (RFC2822)
    
    #OverflowError: mktime argument out of range
    #http://bugs.python.org/issue11850 (python bug)
    date = msg.get('Date')
    if date == None:
        date = ''
    else:    
        pdate = parsedate_tz(date)
        
        if pdate == None:
            date  = ''
        else:
            try:
                date = formatdate(mktime_tz(pdate), usegmt=True)
            except OverflowError:
                date = ''
                pdate = None
    
    #TODO:  the string + coding (client need it for correct representation...)
    usubject = ''
    charSet = 'utf-8'
    subject = msg.get('Subject')
   
    #subject is stored in DB as bytearray
    if subject != None:
        try:
            subject = decode_header(subject)
            
            usubject =''

            for part in subject:
                data, charSet = part
                usubject += data 

            if charSet == None:
                charSet = 'utf-8'

        except:
            usubject = subject 
            
   
    subject = (usubject, charSet)
    
    #subject is (unicode, code) // code is only for client side purpose       
    return (uid, domain, headerFrom, subject, date, pdate)
#    
# meta information about attachments
# ret :
#    boundary for attachment
#    content type of attachment
#    attachment name (filename)
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
        
        #returns unicode string
        fileName = msg.get_filename(None)
        
        if fileName == None:
            fileName = ''
        else:            
            if type(fileName) is unicode:
                fileName = fileName.encode('utf8', 'ignore')
                
            """          
            try:
                fileName = str(msg.get_filename())                
            except UnicodeEncodeError:                
                fileName = msg.get_filename().encode('utf8', 'ignore')
            """
        
        if parentType.lower() == 'message/rfc822':
            attach.append((boundary, parentType, fileName))
        else:
            attach.append((boundary, msg.get_content_type(), fileName))        
#        
def writeAttachment(data):
        
    m = hashlib.sha256()      
    m.update(data)
    key = m.hexdigest()
    
    cass.writeAttachment(key, data)                                 
    
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
                
            if attachments and line[0:len(line) - 1] == attachments[0][0]:                        
                #possible attach boundary        
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
            
            if line == '\n' or line == '\r\n' or line == '\r':            
                buff.fakeNewLine(line)
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
            elif line[0:len(line)-1] in bSet:
                buff.unreadline(line)
                stat = 4
                
            else: #text  
                stat = 2
                         
            if stat == 2 or stat == 4:
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
            
            attKey = writeAttachment(''.join(data))                
            hash = 'DEDUPLICATION:' + attKey + '\n'
            
            body.append(hash)
            
            #create list of attachmentsData -            
            #body.append(''.join(data))
                
            #(name, size, hash), its metadata for messageMetaData CF
            metaData  = (bound[2], len(''.join(data)), attKey)
            attchList.append(metaData)
            
            data = []
            
            if prevStat == 3:
                for newLines in body2:
                    body.append(newLines)
                                                
            stat = 6
                        
            attchWritten += 1
            attachments.pop(0)
            
            buff.unreadline(line)
            
        elif stat == 5:
            #print 'Stat:5'
            
            body.append(line)
            
            if line == '\n' or line == '\r\n' or line == '\r':
                stat = 2
                            
    body = ''.join(body)                  
                            
    if attchTotal != attchWritten:
        #print 'bad email'
        return ()
                            
    return (body, attchList)      

#
def mimeEmail(f, msg, envelope, emailFile):
        
    header = rawHeader(f)    
    metaData = getMetaData(msg)  
    statsData = getStatsData(envelope)
    
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
            
        else:
            print 'Error: Bad email <' + emailFile + '>'
            #
            #write whole email into DB
            body = rawBody(f)
            attach = []
            
            
        cass.writeMetaData(key, metaData, statsData, attach)    
        
            
    #MIME email w/out attachments
    else:        
        body = rawBody(f)

        cass.writeMetaData(key, metaData, statsData, [])    
        
        
    cass.writeContent(key, envelope, header, body)  
#        
def rawEmail(f, msg, envelope):
    
    header = rawHeader(f)
    body = rawBody(f)
    metaData = getMetaData(msg)        
    statsData = getStatsData(envelope)
    
    #metaData[0] is uid (inbox)
    key = createKey(metaData[0], envelope)
    
    cass.writeMetaData(key, metaData, statsData, [])    
    cass.writeContent(key, envelope, header, body)
    
    
##############################################################################

def parseEmail(emailFile):
    
    start = time.time()

    f = open(emailFile, 'r')
    msg = email.message_from_file(f)
    f.seek(0)

    env = open(emailFile + '.envelope', 'r')
    envelope = env.readline()
    env.close()

    try:  
        if msg.is_multipart():
            mimeEmail(f, msg, envelope, emailFile)
        else:
            rawEmail(f, msg, envelope)
        
    except NoBoundaryInMultipartDefect:
        rawEmail(f, msg, envelope)
    

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
