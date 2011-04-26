#!/usr/bin/python

#output = StringIO.StringIO()
#output.write(msg.get_payload(0))
#output.seek(0,0)
#_structure(msg)
import cass
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

cass.writeAttachment('36b67875c42b6b8c5922a70b817fa626951ad8e8', 'aa')