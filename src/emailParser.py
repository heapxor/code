#!/usr/bin/python

import sys
import email
#import cass
import hashlib
import StringIO
from email.errors import NoBoundaryInMultipartDefect
from email.Iterators import _structure
from email.utils import parseaddr
from email.utils import getaddresses

###############################################################################
def rawHeader(key, msg):
    header = []
    
    while True:
        line = msg.readline()
      
        header.append(line)
      
        if line == '\n':
            break
 
    #print ''.join(header)
    #cass.writeHeader(key, ''.join(header))
    return ''.join(header)    
#
def rawBody(key, email):    
    body = []
    
    while True:
        line = email.readline()
        
        body.append(line)
        
        if len(line) == 0:
            break #EOF
            
    # cass.writeBody(key, ''.join(body))
    return ''.join(body)    
#    
def getMetaData(msg):    
    #the real recipient is in the email header, because of tapping
    #X-VF-Scanner-Rcpt-To: x@y
    #TODO:??? normally its in envelope
    uid = msg.get('X-VF-Scanner-Rcpt-To')
    #TODO:??? whats the domain format, fix at the fulltext? 
    domain = uid.partition('@')[2]
    
    eFrom = msg.get('From')
    date = msg.get('Date')
    subject = msg.get('Subject')
    
    return (uid, domain, eFrom, date, subject)
#    
def writeAttachments(msg, boundary, boundaries):    
    if msg.is_multipart():        
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()

        for subpart in msg.get_payload():            
            writeAttachments(subpart, boundary, boundaries)

    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart'):
        
        #??? for sure sha1
        m = hashlib.sha1()
        m.update(msg.get_payload())
        mHash = m.hexdigest()
        boundaries.append((msg.get_filename(), len(msg.get_payload()), mHash, '--' + boundary))
        cass.writeDataAttachment(mHash, msg.get_payload())
#        
def newRawBody(key, f, attachments):    
    body = []
    stat = 3;
    i = 0
    
    while True:      
        #start of attachment data  
        if stat == 1: 
            while True:
                line = f.readline()                
                body.append(line)                
                if line == '\n':
                    body.append('MARK:' + attachments[i][2] + '\n')                    
                    stat = 2
                    break
        #end of attachment data
        elif stat == 2:
            while True: 
                line = f.readline()                
                if  attachments[i][3] + '--' in line:
                    body.append(line)                   
                    stat = 3                    
                    i = i + 1    
                                    
                    if i == len(attachments):
                        stat = 4 
                    break
        #find attachment header
        elif stat == 3:
            while True:
                line = f.readline()             
                body.append(line)
             
                if attachments[i][0] in line:                                
                    stat = 1
                    break
        #last data of email
        elif stat == 4:
            while True:
                line = f.readline()
                body.append(line)
                
                if len(line) == 0:
                    break #EOF
            break                
  
    cass.writeBody(key, ''.join(body))       
#
def mimeEmail(key, f, msg):
       
    header = rawHeader(key, f)
    body = rawBody(key, f)
    metaData = getMetaData(msg)  
    

    #find attachment's boundary and write attachments
    attachments = []
    writeAttachments(msg, 0, attachments)
    
    uid = parseaddr(msg.get('To'))[1]
    domain = uid.partition('@')[2]
    
    cass.writeUid(key, uid, domain)
    cass.writeInboxLast(uid, key)
    
    
    if len(attachments) != 0:
        cass.writeMetaAttachment(key, attachments)
        newRawBody(key, f, attachments)
    else:
        rawBody(key, f)    
#
def rawEmail(key, f, msg):
    
    header = rawHeader(key, f)
    body = rawBody(key, f)
    metaData = getMetaData(msg)        
    
    #cass.writeEmail(key, envelope, header, body, metaData)
##############################################################################
emailFile = sys.argv[1]

f = open(emailFile, 'r')
msg = email.message_from_file(f)
f.seek(0) 

env = open(emailFile + '.envelope', 'r')
envelope = env.readline()
env.close()

mimeEmail(emailFile, f, msg)
#??? whats the key?
"""
try:  
    if msg.is_multipart():
        mimeEmail(emailFile, f, msg)
    else:
        rawEmail(emailFile, f, msg)
        
except NoBoundaryInMultipartDefect:
    rawEmail(emailFile, f, msg)
"""     
f.close()
