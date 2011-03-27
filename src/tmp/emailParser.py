#!/usr/bin/python

import sys
import email
#import cass
import os
import hashlib
#import StringIO
from email.errors import NoBoundaryInMultipartDefect

from email.Iterators import _structure
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
def metaAttachment(msg, boundary, boundaries, bSet, level):    
    if msg.is_multipart():        
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()
            bSet.add('--' + boundary)
            bSet.add('--' + boundary + '--')
            #level = 1
        level = 1
        for subpart in msg.get_payload():
            metaAttachment(subpart, boundary, boundaries, bSet, level)
            level = level + 1

    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart' and msg.get_content_maintype() != 'message'):
                
        boundary = '--' + boundary
        
        try:                            
            fileName = str(msg.get_filename())                
        except UnicodeEncodeError:                
            fileName = msg.get_filename().encode('utf8')
        
        #+1 because of 0A at the end of attachment
        fileSize = len(msg.get_payload()) + 1
        boundaries.append((fileName, fileSize, boundary, level))
        
        
#        
def writeAttachment(data):
    
    #??? for sure sha1
    m = hashlib.sha1()          
    m.update(data) 
    key = m.hexdigest()
                    
    #cass.writeAttachment(key, data)#write data                                 
    return 'MARK:' + key + '\n' 
       
def newRawBody(key, f, attachments, bSet):    
   
    stat = 0;
    i = 0
    
    attachDict = {}
    
    for att in attachments:
        attachDict[att[2]] = 0
        
        
    buff = BufferedData(f)    
    data = []
    body = []

    
    #print 'initial i:' + str(i)

    while True:        
        if stat == 0:
                 
            while True:
                   
                line = buff.readline()                
                body.append(line)
                
                
                
                print '>>>>' + line 
                
                print attachDict
                if attachDict.has_key(line[0:len(line)-1]):
                    print 'has key'
                    val = attachDict[line[0:len(line)-1]] + 1
                    attachDict[line[0:len(line)-1]] = val
                        
                    """
                    print attachments[i]
                    print 'VAL:' + str(val)
                    """           
                    if i < len(attachments) and val == attachments[i][3]:
                    
                        print 'DDDDDDDDDD' + line
                        stat = 1
                        break

                if len(line) == 0:
                    stat = 5
                    break #EOF
                
        #Attachment header
        elif stat == 1:
            while True:
                line = buff.readline()
                
                body.append(line)
                
                if line == '\n':
                    stat = 2                    
                    break
        #body
        elif stat == 2:   
            print line,
            print '2ka'         
            while True:
                line = buff.readline()
            
                if line == '\n':
                    buff.fakeNewLine()
                    stat = 3
                    break
                elif line[0:len(line)-1] in bSet:
                    buff.unreadline(line)
                    #print data,
                    print line
                    stat = 4
                    break
                else:
                    data.append(line)
            
            prevStat = 2        
        elif stat == 3:            
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
            
            for z in range(buff.sizeNewLines()):
                if stat == 4:
                    #print 'do 4ky'
                    body2 = []
                    body2.append(buff.popNewLines())
                else:
                    data.append(buff.popNewLines())                    
                    data.append(line)
                    
            prevStat = 3
        elif stat == 4:
            
            if prevStat == 2:
                
                hash = writeAttachment(''.join(data))
                body.append(hash)
                
                
            elif prevStat == 3:
                
                print 'Z 333'
                hash = writeAttachment(''.join(data))
                
                body.append(hash)
                
                for newLines in body2:
                    body.append(body2.pop())
                    
                #print ''.join(body)
                            
            stat = 0
            i = i + 1        
            
        elif stat == 5:
            break

                    
    return ''.join(body)  

#
def mimeEmail(key, f, msg, envelope, size):
       
    header = rawHeader(key, f)
    metaData = getMetaData(msg)  
    
    #find attachment's boundary and write attachments
    attachments = []
    bSet = set()
    metaAttachment(msg, 0, attachments, bSet, 1)
    
    #print attachments

    if len(attachments) != 0:
        body = newRawBody(key, f, attachments, bSet)
        print body,
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

