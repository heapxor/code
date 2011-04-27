#!/usr/bin/python

# This program define tasks for Celery 
#
#
#


import celery, sys
import hashlib
#sys.path.insert(0, '/usr/local/mailman/bin'

from emailReader import rawEmail
from celery import current_app
from celery.task import task
from celery.task.control import rate_limit
from emailParser import parseEmail
###from esParser import indexEmail
from eventlet import monkey_patch
from emailParser import getMetaData
from emailParser import createKey
import sys
import email
from email import message_from_file

#monkey_patch()

# task which store data into DB (cassandra)
@task(ignore_result=True)
def insertData(fileName):

    logger = insertData.get_logger()
    logger.info("Reading email %s" % fileName)
    logger.info("Task id %s" %  insertData.request.id)

    runTime = parseEmail(fileName)

"""
# task which store data into fulltext search engine
# Index max 150 docs per minute
@task(ignore_result=True, rate_limit="150/m")
def indexData(key):

    logger = insertData.get_logger()
    logger.info("Reading email %s" % fileName)
    logger.info("Task id %s" %  insertData.request.id)

    runTime = indexEmail(fileName)
"""
# task which check data intgerity 
# comparing sha1 hash of local file (email) with email fetched from DB
@task(ignore_result=True, rate_limit="40/s")
def checkData(emailFile):
    
    logger = insertData.get_logger()
    logger.info("Reading email %s" % emailFile)
    logger.info("Task id %s" %  checkData.request.id)

    env = open(emailFile + '.envelope', 'r')
    envelope = env.readline()
    env.close()

    f = open(emailFile, 'r')
    msg = message_from_file(f)
    metaData = getMetaData(msg)
    key = createKey(metaData[0], envelope)
    f.close()

    email = rawEmail(key)
    #print email
    print key
    if email != 0: 

        f = open(emailFile, 'r')
        sEmail = f.readlines()
        f.close()

        m = hashlib.sha1()
        m.update(''.join(sEmail))
        sHash = m.hexdigest()

        m = hashlib.sha1()
        m.update(email)
        dHash = m.hexdigest()
 
        if sHash != dHash:
            logger.info("[Error/EmailTest] Email corrupted.] < %s >" % emailFile)
            #return "0"
        else:
            logger.info("[Email data test: OK]")
            #return "1"

