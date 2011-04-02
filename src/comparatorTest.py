#!/usr/bin/python

import celery, sys
from celery import current_app
from celery.task import task
from celery.task.sets import subtask
import StringIO, hashlib
import emailReader

#
@task
def dataTest(email):

        readDB.delay(email, callback=subtask(readFile, callback=subtask(compare)))

#
@task
def readDB(email, callback=None):

        
        data = emailReader.rawEmail(email[email.rfind('/')+1:])
    
        
        
        m = hashlib.sha1()      
        m.update(data)
        ehash = m.hexdigest()
    
    
        if callback:
                subtask(callback).delay(email, ehash)
#
@task
def readFile(email, ehash, callback=None):


        f = open(email, 'r')

        data= f.readlines()
        
        f.close()
        
        m = hashlib.sha1()      
        m.update(''.join(data))
        
        nhash = m.hexdigest()
    
        if callback:
                subtask(callback).delay(ehash, nhash)
#
@task
def compare(ehash, nhash):

        if ehash == nhash:
                print 'DataTest:[OK]'
        else:
                print 'DataTest:[FALSE]'
