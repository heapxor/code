#!/usr/bin/python

import sys
import email
#import cass
import os
import time
import hashlib
#import StringIO
from email.errors import NoBoundaryInMultipartDefect

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
            line = self.lines.pop()
        
        return line
    
    def fakeNewLine(self):        
        self.newlineStack.append('\n')

    
    def unreadline(self, line):
        self.lines.append(line)

###############################################################################
def rawHeader(key, msg):
    header = []
    
    while True:
        line = msg.readline()
      
        header.append(line)
      
        if line == '\n':
            break
 
    return ''.join(header)    
#
def rawBody(key, email):    
    body = []
    
    while True:
        line = email.readline()
        
        body.append(line)
        
        if len(line) == 0:
            break #EOF
            
    return ''.join(body)    
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

    #Date field in email header is mandatory by RFC2822
    #format: Fri, 18 Mar 2011 16:30:00 +0000
    date = msg.get('Date')
    if date == None:
        date = ''
        
    subject = msg.get('Subject')
    if subject == None:
        subject = '' 
    
    return (uid, domain, eFrom, subject, date)
#    
def metaAttachment(msg, boundary, attach ,bSet):    
    if msg.is_multipart():
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()
            bSet.add('--' + boundary)
            bSet.add('--' + boundary + '--')
            
        for subpart in msg.get_payload():
            metaAttachment(subpart, boundary, attach, bSet)
  
    
    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart' and msg.get_content_maintype() != 'message' and 
            msg.get_content_type() != 'message/rfc822'):
                
        boundary = '--' + boundary
        
        try:                            
            fileName = str(msg.get_filename())                
        except UnicodeEncodeError:                
            fileName = msg.get_filename().encode('utf8')
        
        #size + hash
        attach.append((boundary, msg.get_content_type(), fileName))        
#        
def writeAttachment(data):
    
    #??? for sure sha1
    m = hashlib.sha1()      
    m.update(data)
    key = m.hexdigest()
    
    #cass.writeAttachment(key, data)                                 
    return key 
#
def newRawBody(key, f, attachments, bSet):    
   
    stat = 6;

    buff = BufferedData(f)    
    data = []
    body = []
    bound = ()
    
    while True:       
        if stat == 0:                   
            #print "Stat:0"
            while True:
                line = buff.readline()
                body.append(line)
    
                if line[0:len(line) - 1] == bound[0]:                        
                    #possible attach boundary                      
                    stat = 1
                    break
        #automata start                        
        elif stat == 6:
            #print "Stat:6"            
            if attachments:
                bound = attachments.pop(0)                
                stat = 0
            else:                
                #read end of the email
                stat = 5         
        #attachment header
        elif stat == 1:
            #print "Stat:1"
            while True:
                line = buff.readline()
                
                body.append(line)                
                #is there content-type? / some emails use diff case of content-type
                if 'content-type:' in line.lower():
                    if bound[1] in line:
                        stat = 10
                        break
                    #else:                        
                    #    stat = 0
                    #break                
                if line == '\n':
                    stat = 0            
                    break
        #read rest of the header for selected attachment        
        elif stat == 10:
            #print "Stat:10"
            while True:                
                line = buff.readline()                
                body.append(line)                
                                
                if line == '\n':
                    stat = 2
                    break
        #attachment body (data)
        elif stat == 2:   
            #print "Stat:2"     
            while True:
                line = buff.readline()
            
                if line == '\n':
                    buff.fakeNewLine()
                    stat = 3
                    break
                elif line[0:len(line)-1] in bSet:
                    buff.unreadline(line)
                    stat = 4
                    break
                else:
                    data.append(line)
            prevStat = 2       
        elif stat == 3:
            #print "Stat:3"          
            while True:                
                line = buff.readline()
                
                if line == '\n':
                    buff.fakeNewLine()
                elif line[0:len(line)-1] in bSet:
                    buff.unreadline(line)
                    stat = 4
                    break
                else: #text
                    stat = 2
                    break            
            
            body2 = []            
            for newLine in buff.newlineStack:
                buff.newlineStack.pop()
                if stat == 4:                    
                    body2.append(newLine)                                        
                else:
                    #do 2ky
                    data.append(newLine)
                    
            if stat == 2:
                data.append(line)
                    
            prevStat = 3
        elif stat == 4:
            #print "Stat:4"            
            attKey = writeAttachment(''.join(data))                
            hash = 'DEDUPLICATION:' + attKey + '\n'
            body.append(hash)                
                
            #???build attch list (name, size, hash), its metadata for messageMetaData CF
            
            if prevStat == 3:
                for newLines in body2:
                    body.append(newLines)
                                                
            stat = 6             
        elif stat == 5:
            #print "Stat:5"
            while True:
                line = buff.readline()            
            
                if len(line) == 0:
                    break #EOF
                   
                body.append(line)
                            
            break
                    
    return ''.join(body)
#
def mimeEmail(key, f, msg, envelope, size):
       
    header = rawHeader(key, f)
    metaData = getMetaData(msg)  
    
    #find attachment's boundary and write attachments
    attachments = []
    bSet = set()

    start = time.time()    
    metaAttachment(msg, 0, attachments, bSet)

    if len(attachments) != 0:
        body = newRawBody(key, f, attachments, bSet)
        duration = time.time() - start
        #print header,
        #print body,
    #no attach to deduplicate
    else:
        body = rawBody(key, f)    
        duration = 0 
    #time of email parsing
    return duration
    #cass.writeMetaData(key, envelope, header, size, metaData, attachments)
    #cass.writeContent(key, body)
# 
def rawEmail(key, f, msg, envelope, size):
    
    header = rawHeader(key, f)
    body = rawBody(key, f)
    metaData = getMetaData(msg)        
    attch = []
    
       
    #cass.writeMetaData(key, envelope, header, size, metaData, attch)
    #cass.writeContent(key, body)
##############################################################################


#??? whats the email key?
def parseEmail(emailFile):
    
    f = open(emailFile, 'r')
    msg = email.message_from_file(f)
    f.seek(0)

    env = open(emailFile + '.envelope', 'r')
    envelope = env.readline()
    env.close()

    size = os.path.getsize(emailFile)
    
    try:  
        if msg.is_multipart():
            mimeEmail(emailFile, f, msg, envelope, size)
        else:
            rawEmail(emailFile, f, msg, envelope, size)
        
    except NoBoundaryInMultipartDefect:
        rawEmail(emailFile, f, msg, envelope, size)
     
    f.close()


def main():
    email = sys.argv[1]
    parseEmail(email)


if __name__ == '__main__':
    main()

