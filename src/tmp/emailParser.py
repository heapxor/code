#!/usr/bin/python

import sys
import email
#import cass
import os
import hashlib
#import StringIO
from email.errors import NoBoundaryInMultipartDefect

from email.Iterators import _structure
from email.iterators import body_line_iterator
#from email.utils import parseaddr
#from email.utils import getaddresses

NeedMoreData = object()

class BufferedData():
    
    def __init__(self, f):            
            self.lines = []            
            self.newlineStack = []
            self.f = f
            
    def readline(self):        
        if not self.lines:
            line = self.f.readline()
            #self.lines.append(line)
        else:
            line = self.lines.pop()
        
        return line
    
    def fakeNewLine(self):        
        self.newlineStack.append('\n')
    
    def popNewLines(self):
        return self.newlineStack.pop()
    
    def sizeNewLines(self):
        return len(self.newlineStack)
    
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

    #from je povinny podla RFC
    eFrom = msg.get('From')
    
    if eFrom == None:
        eFrom = 'unknown'

    #date je povinny podla RFC
    #Fri, 18 Mar 2011 16:30:00 +0000
    date = msg.get('Date')
    if date == None:
        date = 'uknown'
        
    subject = msg.get('Subject')
    if subject == None:
        subject = '' 
    
    #prepracovat na list kde kazda polozka je touple (nazov:hodnota)
    return (uid, domain, eFrom, subject, date)
#    
def metaAttachment(msg, boundary, boundaries ,bSet):    
    if msg.is_multipart():        
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()
            bSet.add('--' + boundary)
            bSet.add('--' + boundary + '--')
            
        for subpart in msg.get_payload():
            metaAttachment(subpart, boundary, boundaries, bSet)
  
    print msg.get_content_type()
    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart' and msg.get_content_maintype() != 'message' and msg.get_content_type() != 'text/rfc822-headers'):
                
        boundary = '--' + boundary
        
        try:                            
            fileName = str(msg.get_filename())                
        except UnicodeEncodeError:                
            fileName = msg.get_filename().encode('utf8')
        
        #size + hash
        boundaries.append((boundary, msg.get_content_type(), fileName))        
#        
def writeAttachment(data):
    
    #??? for sure sha1
    m = hashlib.sha1()      
    m.update(data)
    key = m.hexdigest()
    
    #cass.writeAttachment(key, data)#write data                                 
    return 'MARK:' + key + '\n' 
       
def newRawBody(key, f, attachments, bSet):    
   
    stat = 6;

    buff = BufferedData(f)    
    data = []
    body = []
    bound = ()
    
    
    print attachments
    
    while True:       
        if stat == 0:
                   
            print "Stat:0"
            while True:
                line = buff.readline()
                body.append(line)
                
                #print line,
                if line[0:len(line) - 1] == bound[0]:                        
                    #possible attach 
                                        
                    stat = 1
                    break
                
        elif stat == 6:
            print "Stat:6"            
            if attachments:
                bound = attachments.pop(0)
                
                stat = 0
            else:                
                #dojdi subor do konca
                stat = 5         
        #Attachment header
        elif stat == 1:
            print "Stat:1"
            
            #print bound[1]
            while True:
                line = buff.readline()
                
                body.append(line)
                
                
                #print line
                #content type moze byt male/velke to same bound ??
                if 'Content-Type:' in line or 'Content-type:' in line:
                    if bound[1] in line:
                        stat = 10
                    else:
                        
                        stat = 0
                    break
                
                if line == '\n':
                    stat = 0                
                    break
                
        elif stat == 10:
            print "Stat:10"
            #print "inline:" + line
            while True:                
                line = buff.readline()
                
                body.append(line)
                
                #print line,
                
                if line == '\n':
                    stat = 2
                    break
                
        #body
        elif stat == 2:   
            #print line,
            print "Stat:2"     
            while True:
                line = buff.readline()
            
                if line == '\n':
                    buff.fakeNewLine()
                    print 'Chod do trojky'
                    stat = 3
                    break
                elif line[0:len(line)-1] in bSet:
                    buff.unreadline(line)
                    #print data,
                    #print line
                    stat = 4
                    break
                else:
                    data.append(line)
            
            prevStat = 2        
        elif stat == 3:  
            print "Stat:3"          
            while True:                
                line = buff.readline()
                
                if line == '\n':
                    buff.fakeNewLine()
                elif line[0:len(line)-1] in bSet:
                    buff.unreadline(line)
                    stat = 4
                    break
                else:
                    stat = 2
                    break            
            
            print "pocet newlinow:" + str(buff.sizeNewLines())
            
            
            body2 = []
            #for z in range(buff.sizeNewLines()):
            for newLine in buff.newlineStack:
                if stat == 4:
                    #print 'do 4ky'
                    
                    body2.append(newLine)
                    
                else:
                    #do 2ky
                    data.append(newLine)                    
            
            if stat == 2:
                data.append(line)
                    
            prevStat = 3
        elif stat == 4:
            print "Stat:4"
            if prevStat == 2:
                
                hash = writeAttachment(''.join(data))
                body.append(hash)
                
                
            elif prevStat == 3:
                
                print 'Z 333'
                hash = writeAttachment(''.join(data))
                
                body.append(hash)
                
                print 'BODY2 size : ' + str(len(body2))
                for newLines in body2:
                    body.append(newLines)
                    print "KOKOT"                    
                #print ''.join(body)                            
            stat = 6 
            
        elif stat == 5:
            print "Stat:5"
            while True:
                line = buff.readline()            
            
                if len(line) == 0:
                    stat = 7
                    break #EOF
                   
                body.append(line)
                
        elif stat == 7:
            print "Stat:7"
            break
                    
    return ''.join(body)  

#
def mimeEmail(key, f, msg, envelope, size):
       
    header = rawHeader(key, f)
    metaData = getMetaData(msg)  
    
    #find attachment's boundary and write attachments
    attachments = []
    bSet = set()

    metaAttachment(msg, 0, attachments, bSet)

    if len(attachments) != 0:
        body = newRawBody(key, f, attachments, bSet)
        print body
    else:
        body = rawBody(key, f)    
  
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
#emailFile = sys.argv[1]
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


#print cass.getHeader(emailFile)

#print cass.getHeader(emailFile)

