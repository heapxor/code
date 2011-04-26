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

def infoPrint(data):

    print 'From: ' + data['from'] + '|' + 'Subject: ' + data['subject'] + '|' + 'Date: ' + data['eDate'] + '|' + 'Size: ' + data['size'] + '|' + 'Attch: ' + data['attachments'] 


def dumpInbox():
    
    print '?'

# list all (35) messages per inbox  
def listInbox(inbox):
    
    size = 35

    inboxData = cass.getInbox(inbox, size)

    #The key is email key (use it for email dump)
    for key, data in inboxData:
        infoPrint(data)


# print last 20 emails from inbox
def topEmails(key):
    
    l = [] 

    ret = cass.getTop(key, 20)
    
    if ret == None:
        print 'Inbox doesnt exist'
    else:
        for key in ret.itervalues():
            ret = infoEmail(key)
	    l.append(ret)
	
    return l 		

# basic information about email, useful for web email client (like a gmail)
#
# return From, Subject, Data, Size, #attachments
def infoEmail(key):

    if emailCheck(key) == 0:
        print 'Email is not in the DB'
    else:
        data = cass.getEmailInfo(key)
        #infoPrint(data)
	return data
 
#
# raw email
def rawEmail(key):

    #is the email in DB?    
    if emailCheck(key) == 0:
        print 'Email is not in the DB'
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
def main():
    
    arg = sys.argv[1]
    key = sys.argv[2]    
    
    if arg == 'info':
        infoEmail(key)
        
    elif arg == 'raw':
        ret = rawEmail(key)
        # print raw email
        print ret,    
    elif arg == 'top':
        topEmails(key)
           
    elif arg == 'inbox':
        listInbox(key)
        
    else:
        print 'Error: client got bad input parameters'
        sys.exit()
    
#
if __name__ == '__main__':
    
    main()
