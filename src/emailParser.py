#!/usr/bin/python

import sys
import email
#import cass
import os
import time
import hashlib
#import StringIO
from email.errors import NoBoundaryInMultipartDefect

#from email.Iterators import _structure
#from email.iterators import body_line_iterator
#from email.utils import parseaddr
#from email.utils import getaddresses


##################################
## TODO:
##     data expiration (?)
##     message/rfc822
##
## FIXED:
##    windows/unix newline
##    newlines in txt attachment fixed
##    mime body headers with FOLDING 


class BufferedData():
    
    def __init__(self, f):            
            self.lines = []            
            self.newlineStack = []
            self.f = f
            
    def readline(self):        
        if not self.lines:
            line = self.f.readline()
            line = line.replace('\r\n', '\n').replace('\r', '\n')
       
        else:
            line = self.lines.pop(0)
        
        return line
    
    def fakeNewLine(self):        
        self.newlineStack.append('\n')

    
    def unreadline(self, line):
        self.lines.append(line)

###############################################################################
def rawHeader(key, msg):
    header = []
    
    while True:
        line = msg.readline()
        line = line.replace('\r\n', '\n').replace('\r', '\n')
      
        header.append(line)
      
        if line == '\n':
            break
 
        #because the body is optional
        if len(line) == 0:
            break #EOF
    return ''.join(header)
#
def rawBody(key, email):    
    body = []
    
    while True:
        line = email.readline()
        line = line.replace('\r\n', '\n').replace('\r', '\n')
        
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

    #From field in email header is mandatory by RFC2822
    eFrom = msg.get('From')
    
    if eFrom == None:
        eFrom = ''

    #Date field in email header is mandatory by RFC2822
    #format: Fri, 18 Mar 2011 16:30:00 +0000
    date = msg.get('Date')
    if date == None:
        date = ''
        
    subject = msg.get('Subject')
    if subject == None:
        subject = '' 
    
    return (uid, domain, eFrom, subject, date)
#    
def metaAttachment(msg, parentType, boundary, attach ,bSet):
    
#    parentType = ""
        
    if msg.is_multipart():
        if  (msg.get_content_maintype() == 'multipart'):
            boundary = msg.get_boundary()
            bSet.add('--' + boundary)
            bSet.add('--' + boundary + '--')
        
        parentType = msg.get_content_type()
        
        for subpart in msg.get_payload():
            metaAttachment(subpart, parentType, boundary, attach, bSet)
  
    
    
    if (msg.get_content_type() != 'text/plain' and msg.get_content_type() != 'text/html' and
            msg.get_content_maintype() != 'multipart' and msg.get_content_maintype() != 'message' and 
            msg.get_content_type() != 'message/rfc822'):
                
        boundary = '--' + boundary
        
        #print parentType
        #print boundary
        print msg.get_content_type()
        
        
        try:                            
            fileName = str(msg.get_filename())                
        except UnicodeEncodeError:                
            fileName = msg.get_filename().encode('utf8')
        
        #size + hash
        if parentType.lower() == 'message/rfc822':
            attach.append((boundary, parentType, fileName))
        else:
            attach.append((boundary, msg.get_content_type(), fileName))        
#        
def writeAttachment(data):
    
    #??? for sure sha1
    m = hashlib.sha1()      
    m.update(data)
    key = m.hexdigest()
    
    #print key
    ###cass.writeAttachment(key, data)                                 
    return key 
#

def newRawBody(key, f, attachments, bSet):    
   
    stat = 6;

    buff = BufferedData(f)    
    data = []
    body = []
    bound = ()
    attchList = []
    badEmail = True    
    boundaryHeader = 0
    attchTotal = len(attachments)
    attchWritten = 0
    #print attchTotal
    
    print attachments
    
    while True:
        
        line = buff.readline()
        
        if len(line) == 0:
            #print 'EOF' 
            break        
     
        #automata start                        
        if stat == 6:
            #print 'Stat:6'
            body.append(line)
            
            #if attchWritten > 2 and bound[2] == '08022011222.jpg':
                #print '>>>>' + line,
                
            if attachments and line[0:len(line) - 1] == attachments[0][0]:                        
                #possible attach boundary        
                #attachments.pop(0)      
                #print line      
                bound = attachments[0]                
                stat = 1
                                  
        elif stat == 1:
            #print "Stat:1"
            
            body.append(line)                           
            #is there content-type? / some emails use diff *case of content-type
            if 'content-type:' in line.lower():              
                if bound[1] in line.lower():         
                    boundaryHeader = 1
                else:
                    #possible folding in header field
                    
                    line = buff.readline()
                    #print repr(line[0]),
                    if line[0] == '\t' or line[0] == ' ':
                        body.append(line)
                                
                        if bound[1] in line.lower():
                            boundaryHeader = 1                                     
                    else:                        
                        buff.unreadline(line)
            elif line == '\n':                                
                if boundaryHeader == 1:
                    
                    if bound[1].lower() == 'message/rfc822':
                    
                        stat = 5
                    else:
                        stat = 2
                else:
                    stat = 6      
                    
        elif stat == 2:   
            #print "Stat:2" 
            boundaryHeader = 0
            
            if line == '\n':
                #print repr(line)
                buff.fakeNewLine()
                #print buff.newlineStack
                stat = 3                
            elif line[0:len(line)-1] in bSet:
                buff.unreadline(line)
                stat = 4                
            else:
                data.append(line)
                
            prevStat = 2           
                       
        elif stat == 3:
            #print "Stat:3"          

            if line == '\n':                    
                buff.fakeNewLine()
                #print buff.newlineStack
            elif line[0:len(line)-1] in bSet:
                buff.unreadline(line)
                stat = 4
                
            else: #text  
                stat = 2
                         
            if stat == 2 or stat == 4:        
                #print 'cistim stack:' + str(len(buff.newlineStack))
                body2 = []
                for newLine in buff.newlineStack:
                    if stat == 4:                    
                        body2.append(newLine)                                        
                    else:
                        data.append(newLine)
                    
                buff.newlineStack = []        
                if stat == 2:
                    data.append(line)
            
            prevStat = 3
            
        elif stat == 4:
            print "Stat:4"          
            #ddata = ''.join(data)  
            attKey = writeAttachment(''.join(data))                
            hash = 'DEDUPLICATION:' + attKey + '\n'
            
            body.append(hash)
            
            #create list of attachmentsData -
            
            #body.append(''.join(data))
                
            #(name, size, hash), its metadata for messageMetaData CF
            #metaData  = (bound[2], len(ddata), attKey)
            metaData  = (bound[2], len(''.join(data)), attKey)
            attchList.append(metaData)
            
            data = []
            
            if prevStat == 3:
                for newLines in body2:
                    body.append(newLines)
                                                
            stat = 6
            
            #print 'att written:' + bound[2]

            
            attchWritten += 1
            attachments.pop(0)
            
            buff.unreadline(line)
            
            
        elif stat == 5:
            #print line
            #print 'Stat:5'
            
            body.append(line)
            
            if line == '\n':
                stat = 2
     
                            
                            
                            
    if attchTotal != attchWritten:
        #print 'bad email'
        return ()
                            
    return (''.join(body), attchList)      
    
"""    
    while True and badEmail:       
        if stat == 0:                   
            #print "Stat:0"
            while True:
                line = buff.readline()
                body.append(line)
                #print line,
                if line[0:len(line) - 1] == bound[0]:                        
                    #possible attach boundary       
                    #print line               
                    stat = 1
                    break
        #automata start                        
        elif stat == 6:
            #print "Stat:6"            
            if attachments:
                bound = attachments.pop(0)                
                stat = 0
            else:                
                #read end of the email
                stat = 5         
        #attachment header
        elif stat == 1:
            #print "Stat:1"
            fold = 0
            #??? fix that dirt...bez rozdielu ci som v spravnom headri tak dojdi ho az do konca? 
            while True:
                #print '>>>>'
                line = buff.readline()
                #print line, 
                body.append(line)                
                #is there content-type? / some emails use diff case of content-type
                if 'content-type:' in line.lower():
                    
                    if bound[1] in line.lower():
                        
                        stat = 10
                        break
                    #else:                        
                    #    stat = 0
                    #break
                    else:
                        #print 'folding'
                        #possible folding in header field                        
                        while True:
                            line = buff.readline()
                            
                            #folding
                            #print repr(line[0]),
                            if line[0] == '\t' or line[0] == ' ':
                                
                                
                                body.append(line)
                                
                                #     print 'bound:' + bound[1]
                                #     print line
                                if bound[1] in line.lower():
                                    #        print 'yup'
                                    stat = 10
                                    fold = 1
                                    break
                                else:
                                    break
                            else:                                
                                break
                            
                            buff.unreadline(line)
                         
                        #print fold
                         
                if line == '\n':
                    stat = 0            
                    break                
                elif fold == 1:
                    stat = 10
                    break
        #read rest of the header for selected attachment        
        elif stat == 10:
            #print "Stat:10"
            while True:                
                line = buff.readline()                
                body.append(line)                
                                
                if line == '\n':
                    stat = 2
                    break
            #print line
        #attachment body (data)
        elif stat == 2:   
            #print "Stat:2"     
            while True:
                line = buff.readline()

                if line == '\n':
                    #print repr(line)
                    buff.fakeNewLine()
                    #print buff.newlineStack
                    stat = 3
                    break
                elif line[0:len(line)-1] in bSet:
                    buff.unreadline(line)
                    stat = 4
                    break
                else:
                    data.append(line)
            prevStat = 2
            #print line
                       
        elif stat == 3:
            #print "Stat:3"          
            while True:                
                line = buff.readline()
                #print repr(line)
                if line == '\n':                    
                    buff.fakeNewLine()
                    #print buff.newlineStack
                elif line[0:len(line)-1] in bSet:
                    buff.unreadline(line)
                    stat = 4
                    break
                else: #text
                    #print 'TEXT?'
                    #print repr(line)
                    
                    stat = 2
                    break            
            
            #print 'cistim stack:' + str(len(buff.newlineStack))
            body2 = []
            #print "stack:"     
            #print buff.newlineStack       
            for newLine in buff.newlineStack:
                #print 'for'
                
                if stat == 4:                    
                    body2.append(newLine)                                        
                else:
                    #do 2ky
                    #print 'do 2'
                    data.append(newLine)
                #buff.newlineStack.pop()
                
            buff.newlineStack = []        
            if stat == 2:
                data.append(line)
            #print 'opustam 3'
            prevStat = 3
        elif stat == 4:
            #print "Stat:4"          
            
            #ddata = ''.join(data)  
            attKey = writeAttachment(''.join(data))                
            hash = 'DEDUPLICATION:' + attKey + '\n'
            
            ###body.append(hash) uncomment
            
            body.append(''.join(data))
                            
            
            #print ''.join(data)
            #print '>>>>>>>>>>>>>>>>>>'
                
            #(name, size, hash), its metadata for messageMetaData CF
            #metaData  = (bound[2], len(ddata), attKey)
            metaData  = (bound[2], len(''.join(data)), attKey)
            attchList.append(metaData)
            
            data = []
            
            if prevStat == 3:
                for newLines in body2:
                    body.append(newLines)
                                                
            stat = 6
        elif stat == 5:
            #print "Stat:5"
            while True:
                line = buff.readline()            
            
                if len(line) == 0:
                    break #EOF
                   
                body.append(line)
                            
            break
"""

#
def mimeEmail(key, f, msg, envelope, size):
       
    header = rawHeader(key, f)
    #print header,
    metaData = getMetaData(msg)  
    
    #find attachment's boundary and write attachments
    attachments = []
    bSet = set()

    metaAttachment(msg, "", 0, attachments, bSet)

    if len(attachments) != 0:
        #print attachments
#        (body, attach) = newRawBody(key, f, attachments, bSet)
        ret = newRawBody(key, f, attachments, bSet)
        if ret:
            (body, attach) = ret
            ###cass.writeMetaData(key, envelope, header, size, metaData, attach)    
            ###cass.writeContent(key, body)
        else:
            print 'Error: Bad email <' + key + '>'
        
	#print header,
        #print body,
    #no attach to deduplicate
    else:
        body = rawBody(key, f)    
    #time of email parsing
    #return duration
#        
def rawEmail(key, f, msg, envelope, size):
    
    header = rawHeader(key, f)
    body = rawBody(key, f)
    metaData = getMetaData(msg)        
    #attch = []
       
    ###cass.writeMetaData(key, envelope, header, size, metaData, [])
    ###cass.writeContent(key, body)
##############################################################################


#??? whats the email key?
def parseEmail(emailFile):
    
    start = time.time()

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

    duration = time.time() - start 
   
    return duration

def main():
    
    email = sys.argv[1]
    start = time.time()
    
    parseEmail(email)
    
    
    duration = time.time() - start

    #print duration

if __name__ == '__main__':
    main()

