#!/usr/bin/python

import sys
import email
import os
import hashlib
#import StringIO
from email.errors import NoBoundaryInMultipartDefect

#from email.Iterators import _structure
#from email.utils import parseaddr
#from email.utils import getaddresses

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
def metaAttachment(msg, boundary, boundaries):    
    if msg.is_multipart():        
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()

        for subpart in msg.get_payload():            
            metaAttachment(subpart, boundary, boundaries)

    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart' and msg.get_content_maintype() != 'message'):
                
        boundary = '--' + boundary
        
        try:                            
            fileName = str(msg.get_filename())                
        except UnicodeEncodeError:                
            fileName = msg.get_filename().encode('utf8')
        
        #+1 because of 0A at the end of attachment
        fileSize = len(msg.get_payload()) + 1
        boundaries.append((fileName, fileSize, boundary, msg.get_content_type()))
        #cass.writeAttachment(mHash, msg.get_payload())
#        
def writeAttachment(key, data):
    
    #write data
    print 'a'
            
def newRawBody(key, f, attachments):    
    body = []
    stat = 3;
    i = 0
    
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
                    #we got right attachment data
                    if att == 1:                       
                        stat = 2
                    else:
                        #its html/text/message/...
                        stat = 3
                    break
        #read attachment data
        elif stat == 2:
            att = []
            while True: 
                line = f.readline()  
                              
                if  attachments[i][2] + '--' in line:
                    
                    data = ''.join(att)    
                    
                    #??? for sure sha1
                    m = hashlib.sha1()          
                    m.update(data) 
                    mHash = m.hexdigest()
                    
                    writeAttachment(mHash, data)                    
                    body.append('MARK:' + mHash + '\n')                    
                    body.append(line)   
                     
                    stat = 3                    
                    i = i + 1    
                    
                    if i == len(attachments):
                        stat = 4 
                    break
                else:
                    att.append(line)
                    
        #find possible attachment boundary
        elif stat == 3:
            while True:
                line = f.readline()             
                body.append(line)
                
                if attachments[i][2] in line:
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
  
    return ''.join(body)       
#
def mimeEmail(key, f, msg, envelope, size):
       
    header = rawHeader(key, f)
    metaData = getMetaData(msg)  
    
    #find attachment's boundary and write attachments
    attachments = []
    metaAttachment(msg, 0, attachments)
    
    if len(attachments) != 0:
        body = newRawBody(key, f, attachments)
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

#??? whats the key?


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
