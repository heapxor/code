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

#format output message of email details
def infoPrint(data):
    
    if 'attachmnets' in data:
	att = data['attachments']
    else:
	att = 0
    
    if 'from' in data:
	hfrom = data['from']
    else:
	hfrom = 'empty'

    if 'subject' in data:
	subject = data['subject']
    else:
	subject = 'empty'

    if 'hDate' in data:
	date = data['hDate']
    else:
	date = 'empty'

    if 'size' in data:
	size = data['size']
    else:
	size = 0
	#print data
   
    
    print "From: %s | Subject: %s | Date: %s | Size: %d | Attchs: %d" % (hfrom, subject, date, size, att) 
	
#
# list #numb messages from user inbox 
def listInbox(inbox, numb):

    inboxData = cass.getInbox(inbox, numb)

    #The key is email key (use it for email dump)
    for key, data in inboxData:
        infoPrint(data)


#
# print last #numb emails from inbox (sorted by Date from email header)
#
def topEmails(key, numb):
    
    ret = cass.getTop(key, numb)
    
    if ret == None:
        print 'Inbox doesnt exist'
    else:
        for key in ret.itervalues():
            infoEmail(key)


# basic information about email, useful for web email client (like a gmail)
#
# return From, Subject, Date, Size, #attachments
def infoEmail(key):

    if emailCheck(key) == 0:
        print 'Email is not in the DB'
    else:
        data = cass.getEmailInfo(key)
        infoPrint(data)
 
#
# get raw email
# 
# ret:
#     raw string

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
	numb = sys.argv[3]
        topEmails(key, int(numb))
           
    elif arg == 'inbox':
	numb = sys.argv[3]
        listInbox(key, int(numb))
        
    else:
        print 'Error: client got bad input parameters'
        sys.exit()
    
#
if __name__ == '__main__':
    
    main()
