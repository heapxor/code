#!/usr/bin/python

from emailParser import parseEmail
from emailReader import rawEmail

import celery, sys

from celery import current_app
from celery.task import task

import hashlib

#@task(ignore_result=True)
@task
def insertData(fileName):

    logger = insertData.get_logger()
    logger.info("Reading email %s" % fileName)
    logger.info("Task id %s" %  insertData.request.id)

    runTime = parseEmail(fileName)
        
    #print "Running time %s" % runTime
    return runTime
    
    
@task
def readData(key):
    
    f = open('', 'r')
    sEmail = f.readlines()
    f.close()
    
    sHash = hashlib.new(''.join(sEmail))
    sHash = sHash.hexdigest()
    
    email = rawEmail(key)
    
    dHash = hashlib.new(email)
    
    if sHash != dHash:
        print '[Error/EmailTest] Email corupted.'
        
