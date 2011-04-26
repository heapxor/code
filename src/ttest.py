#!/usr/bin/python

import celery, sys

from celery import current_app
from celery.task import task
from celery.task.sets import subtask
from celery.task import chord

@task
def dataTest(email):
	
		
	readDB.delay(email, callback=subtask(readFile, callback=subtask(compare)))


@task 
def readDB(email, callback=None):
	
# 	ehash = 

	if callback:
		subtask(callback).delay(email, ehash)

@task
def readFile(email, ehash, callback=None):
	
	
	f = open(email, 'r')
	
	
	
	f.close()

	if callback:
		subtask(callback).delay(ehash, nhash)


@task
def compare(ehash, nhash):
	
	if ehash == nhash:
		print 'DataTest:[OK]'
	else:
		print 'DataTest:[FALSE]'


