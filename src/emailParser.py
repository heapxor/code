#!/usr/bin/python

import sys
import email
import cass
from email.iterators import _structure


def rawHeader(key, msg):

    header = []
    while True:
        line = msg.readline()
      
        header.append(line)
      
        if line == '\n':
            break
 
    cass.insert(key, ''.join(header), 'Header')

#
def rawEnvelope(key, envelope):
    
    line = envelope.readline()
    
    cass.insert(key, line, 'Envelope')

#
def rawEmail(emailFile):
  
    email = open(emailFile, 'r')
    envelope = open(emailFile + '.envelope', 'r')
  
    rawHeader(emailFile, email)
    rawEnvelope(emailFile, envelope)

    #zapis body !!! - 1MB chunks

    email.close()
    envelope.close()
#

def email_structure(msg, boundary, boundaries):
    
    if msg.is_multipart():
        
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()

        for subpart in msg.get_payload():
            
            email_structure(subpart, boundary, boundaries)

    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart'):
        print boundary
        print msg.get_filename()
        
    
    
def parseEmail(msg):
    print 'b'
    
    
  
#
def mimeEmail(emailFile):  
    
    print 'b'



###
emailFile = sys.argv[1]

f = open(emailFile, 'r')
msg = email.message_from_file(f)
f.close()

boundaries = []
email_structure(msg, 0, boundaries)





"""
#try:  
if msg.is_multipart():
    parseEmail(msg)
    mimeEmail(emailFile)
else:
    rawEmail(emailFile)

    
#   except NoBoundaryInMultipartDefect:
#       rawEmail(emailFile)
        
"""
