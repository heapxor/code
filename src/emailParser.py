#!/usr/bin/python

import sys
import email
import cass
import hashlib
import StringIO
from email.errors import NoBoundaryInMultipartDefect


def rawHeader(key, msg):

    header = []
    
    while True:
        line = msg.readline()
      
        header.append(line)
      
        if line == '\n':
            break
 
    #cass.writeHeader(key, ''.join(header))    
#
def rawBody(key, email):
    
    body = []
    
    while True:
        line = email.readline()
        
        body.append(line)
        
        if len(line) == 0:
            break #EOF
            
    #cass.writeBody(key, ''.join(body))
    
#
def rawEnvelope(key, envelope):
    
    line = envelope.readline()
    
    #cass.writeEnevelope(key, line)

#
def writeAttachments(msg, boundary, boundaries):
    
    if msg.is_multipart():        
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()

        for subpart in msg.get_payload():            
            writeAttachments(subpart, boundary, boundaries)

    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart'):
        
        m = hashlib.sha1()
        m.update(msg.get_payload())
        mHash = m.hexdigest()
        boundaries.append((msg.get_filename(), len(msg.get_payload()), mHash, '--' + boundary))
        #cass.writeAttachment(mHash, msg.get_payload()))
        
#    
def rawEmail(key, email):
  
    rawHeader(key, email)
    rawBody(key, email)

#    
def newRawBody(key, f, attachments):
    attchHeader = []
    
    while True:
        
        line = f.readline() 
        attchHeader.append(line)
        
        if line == '\n':
            break
        
    #zapis attachment

    #attchHeader.append(attchHash)
    
    #cass.write(key, body, attachments)

#
"""
def writeAttachments(key, msg, attachments):
    #write attachments - mozem to robit uz v get_attach_info
    for part in msg.walk():
        # multipart/* are just containers
        if part.get_content_maintype() == 'multipart':
            continue
        
        filename = part.get_filename()
        if filename:
                    
            part.get_payload(decode=True)
"""            
#
def mimeEmail(key, f, msg):  

    rawHeader(key, f)

    #find attachment's boundary
    attachments = []
    writeAttachments(msg, 0, attachments)
    
    if len(attachments) != 0:
        newRawBody(key, f, attachments)                
        #writeAttachments(key, msg, attachments)
    else:
        rawBody(key, f)
    
#    
"""
    if len(attachments) != 0:    
    
        body = []
    
        for i in attachments:
        
            boundary = i[2]
        
            while True:
                line = email.readline()
            
                if line != boundary:
                    body.append(line)
                else:
                    attHeader = writeAttachment(email, i)
                    body.append(attHeader)
                    
    else:
        rawBody(key, email)
"""   
##############################################################################
emailFile = sys.argv[1]

f = open(emailFile, 'r')
msg = email.message_from_file(f)

f.seek(0)

"""
envelope = open(emailFile + '.envelope', 'r')
rawEnvelope(emailFile, envelope)
envelope.close()
"""

#output = StringIO.StringIO()
#output.write(msg.get_payload(0))
#output.seek(0,0)


mimeEmail(emailFile, f, msg)



"""

try:  
    if msg.is_multipart():
        mimeEmail(emailFile, f, msg)
    else:
        rawEmail(emailFile, f)
        
except NoBoundaryInMultipartDefect:
    rawEmail(emailFile, f)
        
f.close()
"""