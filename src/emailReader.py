#!/usr/bin/python

#################
##  TODO:
##
##

import sys
import email
import cass
import hashlib 
from cass import emailCheck


# basic information about email, useful for web email client (like a gmail)
#
# return From, Subject, Data, Size, #attachments
def infoEmail(key):

    if emailCheck(key) == 0:
        print 'Email is not in the DB'
    else:
        data = cass.getEmailInfo(key)
    
        #what about data.keys() ?
        print 'From: ' + data['from'] + '|' + 'Subject: ' + data['subject'] + '|' + 'Date: ' + data['hDate'] + '|' + 'Size: ' + data['size'] + '|' + 'Attch: ' + data['attachments']   
    
#
# raw email
def rawEmail(key):

    #is the email in DB?    
    if emailCheck(key) == 0:
        #return 'Email is not in the DB'
        return 0
    else:    
        header = cass.getRawHeader(key)        
        #mime or not mime email
        mime = cass.getMimeInfo(key)
        
        body = {}
        if mime == 0:
            #get the raw body
            body = cass.getRawBody(key) 
        else:
            #get the raw MIME body
            body = cass.getMimeBody(key, mime)

        return header + body
        
#
def attachEmail(key):
    
    print ''
    
#
def main():
    
    arg = sys.argv[1]
    key = sys.argv[2]    
    
    if arg == 'info':
        infoEmail(key)
        
    elif arg == 'attach':
        attachEmail(key)
        
    elif arg == 'raw':
        ret = rawEmail(key)
        # print raw email
        print ret,
        
    else:
        print 'Error: client got bad input parameters'
        sys.exit()
    
#
if __name__ == '__main__':
    
    main()
