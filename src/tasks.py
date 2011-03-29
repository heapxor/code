from emailParser import parseEmail
import celery, sys

from celery import current_app
from celery.task import task


#@task(ignore_result=True)
@task
def insertData(fileName):

    logger = insertData.get_logger()
    logger.info("Reading email %s" % fileName)
    logger.info("Task id %s" %  insertData.request.id)

    runTime = parseEmail(fileName)
    #print "Running time %s" % runTime
    return runTime

