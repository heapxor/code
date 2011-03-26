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

class BufferedData(object):
    
    def __init__(self):
            self.partial = ''
            
            self.lines = []
            
            self.oefstack = []
            

    def readline(self):
        
        line = self.lines.pop()
        
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
def writeAttachment(key, data):
    
    #cass.writeAttachment(key, data)#write data
    print ''
            
def newRawBody(key, f, attachments, bSet):    
    body = []
    stat = 0;
    i = 0
    ret = 0 

    print attachments
    
    while True:
        
        if stat == 0:            
            while True:
                line = f.readline()               
                body.append(line)
                
                if line.startswith(attachments[i][2]):
                    ret = ret + 1
                
                if ret == attachments[i][3]:
                    stat = 1
                    ret = 0
                    break
        #header
        elif stat == 1:
            while True:
                line = f.readline()               
                body.append(line)   
                
                if line == '\n':
                    stat = 2
                    
                    break
        #body
        elif stat == 2:
            data = []
            
            print bSet
            while True:
                line = f.readline()    
                
                #print line[0:len(line)-1]
                bound = line[0:len(line)-1] in bSet
                           
                    
                if bound:
                    
                    print 'DDDD'
                    
                    attch = ''.join(data)
                    
                    print attch,
                    
                    i = i + 1                    
                    
                    """
                    print len(line)
                    print line[0:len(line)-1],
                    """
                    print len(attch)
                    stat = 3
                    
                    if i == len(attachments):                        
                        stat = 4
                    break
                    
                else:
                    data.append(line)
                    
        elif stat == 3:
            print stat 
            if line[0:len(line)-1] == attachments[i][2] and attachments[i][3] == 1:
                stat = 2
            else:
                stat = 0
            
            break
        elif stat == 4:
            print stat 
            while True:                                
                line = f.readline()
                body.append(line)
                
                if len(line) == 0:
                    break #EOF
            break
        
    return ''.join(body)  

"""    
    while True:      
        #start of attachment data  
        if stat == 1: 
            att = 0
            while True:
                line = f.readline()               
                body.append(line)
                
                if 'Content-Type:' in line:
                    if attachments[i][3] in line:                    
                        att = 1
                        
                if line == '\n':
                    #start of our attachment data
                    if att == 1:      
                        stat = 2
                    else:
                        #its html/text/message/...
                        stat = 3
                        
                    print stat 
                    break
        #read attachment data
        elif stat == 2:
            att = []
            
            
            while True: 
                line = f.readline()
                 
                bound = line[0:len(line)-1] in bSet
                
                
                if line == '\n' or bound:
                                
                    data = ''.join(att)
                
                    print line,
                
                    if bound == True:
                        print 'in set' 
                    
                    #??? for sure sha1
                    m = hashlib.sha1()          
                    m.update(data) 
                    mHash = m.hexdigest()
                    
                    #writeAttachment(mHash, data)
                                 
                    body.append('MARK:' + mHash + '\n')                    
                    body.append(line)   
                    
                    stat = 3                    
                    i = i + 1    
                    
    
                    if bound:
                        stat = 5
                    
                    if i == len(attachments):
                        stat = 4
                     
                    print 'a:' + i 
                    print 'a:' + len(attachments)
                    print stat 
                    break
                else:
                    att.append(line)                    
        #find possible attachment boundary
        elif stat == 3:
            while True:
                line = f.readline()             
                body.append(line)
                
                if line.startswith(attachments[i][2]):
                    stat = 1
                    break                
        #remain data of email
        elif stat == 4:
            while True:
                                
                line = f.readline()
                body.append(line)
                
                
                if len(line) == 0:
                    break #EOF
            break                
        elif stat == 5:
            if line.startswith(attachments[i][2]):
                    stat = 1                    
            else:
                stat = 3
            
            print stat
 """           
     
                
         
#
def mimeEmail(key, f, msg, envelope, size):
       
    header = rawHeader(key, f)
    metaData = getMetaData(msg)  
    
    #find attachment's boundary and write attachments
    attachments = []
    bSet = set()
    metaAttachment(msg, 0, attachments, bSet, 1)
    
    #print attachments
    """
    _structure(msg)
    print '0:'
    _structure(msg.get_payload(0))
    print '1'
    _structure(msg.get_payload(1))
    print 'sub'
    _structure(msg.get_payload(1).get_payload(0))
    _structure(msg.get_payload(1).get_payload(0).get_payload(1))
    
    print '2'
    _structure(msg.get_payload(2))
    """    
  
    if len(attachments) != 0:
        body = newRawBody(key, f, attachments, bSet)
        #print body,
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
    
    print 'bbb'   
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

