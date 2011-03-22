from emailParser import parseEmail
import celery
from celery.task import task
import sys




@task
def insertData(fileName):
    parseEmail(fileName)

