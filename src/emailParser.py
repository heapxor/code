#!/usr/bin/python

import sys
import email
import cass
import hashlib
import StringIO
from email.errors import NoBoundaryInMultipartDefect
from email.Iterators import _structure
###############################################################################
def rawHeader(key, msg):
    header = []
    
    while True:
        line = msg.readline()
      
        header.append(line)
      
        if line == '\n':
            break
 
    #print ''.join(header)
    cass.writeHeader(key, ''.join(header))    
#
def rawBody(key, email):    
    body = []
    
    while True:
        line = email.readline()
        
        body.append(line)
        
        if len(line) == 0:
            break #EOF
            
    cass.writeBody(key, ''.join(body))    
#
def rawEnvelope(key, envelope):
    
    line = envelope.readline()
    
    cass.writeEnevelope(key, line)
#    
def rawEmail(key, email):  
    rawHeader(key, email)
    rawBody(key, email)
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
    rawHeader(key, f)

    #find attachment's boundary and write attachments
    attachments = []
    writeAttachments(msg, 0, attachments)
    
    if len(attachments) != 0:
        cass.writeMetaAttachment(key, attachments)
        newRawBody(key, f, attachments)
    else:
        rawBody(key, f)    
#
##############################################################################
emailFile = sys.argv[1]

f = open(emailFile, 'r')
msg = email.message_from_file(f)
f.seek(0) 

envelope = open(emailFile + '.envelope', 'r')
rawEnvelope(emailFile, envelope)
envelope.close()


try:  
    if msg.is_multipart():
        mimeEmail(emailFile, f, msg)
    else:
        rawEmail(emailFile, f)
        
except NoBoundaryInMultipartDefect:
    rawEmail(emailFile, f)
        
        
f.close()
