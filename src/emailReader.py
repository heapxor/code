#!/usr/bin/python

import sys
import email
import cass
import hashlib 

#################
#  TODO:
#
#  reading info email (attch??)
#

def infoEmail(key):

    data = cass.getEmailInfo(key)
    
    #what about data.keys() ?
    print 'From: ' + data['from'] + '|' + 'Subject: ' + data['subject'] + '|' + 'Date: ' + data['date'] + '|' + 'Size: ' + data['size'] + '|' + 'Attch: ' + data['attachments']
    
    
#???
def attachEmail(key):
    
    print ''
    
def rawEmail(key):

    
    header = cass.getRawHeader(key)
    
    #mime or not mime email
    mime = cass.getMimeInfo(key)
    
    body = {}
    if mime == 0:
        #get the whole body
        body = cass.getRawBody(key)
        
    else:
        #body & repair attach
        body = cass.getMimeBody(key, mime)
         
    #check(header+body, key)

    return header + body
    
"""
def check(data, key):

    f = open(key, 'r')
    sEmail = f.readlines()
    f.close()
    
    m = hashlib.sha1()
    m.update(''.join(sEmail))
    sHash = m.hexdigest()
    
    email = data
    
    m = hashlib.sha1()
    m.update(email)
    dHash = m.hexdigest()

    print sHash
    print dHash

"""

def main():
    
    
    arg = sys.argv[1]
    key = sys.argv[2]
    
    
    if arg == 'info':
        infoEmail(key)
    elif arg == 'attach':
        attachEmail(key)
    elif arg == 'raw':
        ret = rawEmail(key)
    else:
        print 'client error: bad input parameter'
        sys.exit()
    
    print ret,

if __name__ == '__main__':
    
    main()

