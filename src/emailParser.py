#!/usr/bin/python

import sys
import email
import cass
#

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
def parseEmail(msg):
    
    print 'a'
  
#
def mimeEmail(emailFile):  
    
    print 'b'



###

emailFile = sys.argv[1]

f = open(emailFile, 'r')
msg = email.message_from_file(f)
f.close()

#try:  
if msg.is_multipart():
    parseEmail(msg)
    mimeEmail(emailFile)
else:
    rawEmail(emailFile)

    
#   except NoBoundaryInMultipartDefect:
#       rawEmail(emailFile)
        

