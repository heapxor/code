#!/usr/bin/python

########
#  Purpose of this program is to create celery task for storing data into database,
#  fulltext engine and for data integrity check.
#  
#  Data is read from local filesystem.
# 
#  insertData - store email into database (cassandra)
#  indexData  - store email into fulltext engine (ES)
#  checkData  - check data integrity (local file vs. data stored in db) 
#

import os, sys, time

from os.path import join, getsize
from tasks import insertData
from tasks import checkData
from celery.exceptions import TimeLimitExceeded
from celery.utils import gen_unique_id
from celery.result import TaskSetResult
from celery import current_app
from celery.task.control import rate_limit

                           
def main():

    emailStats = [0, 0]
 
    #connection pool for the broker
    tset = TaskSetResult(gen_unique_id(), [])
    connection = current_app.broker_connection()
    publisher = current_app.amqp.TaskPublisher(connection)

    start = time.time()

    try:
        for root, dirs, files in os.walk('/big/mails'):
            for name in files:
                email = join(root,name)
	        
                if not 'envelope' in email:
                    size = getsize(email)
                    emailStats[0] += 1
                    emailStats[1] += size

                    insertData.apply_async((email, ), publisher=publisher)
                    #checkData.apply_async((email,), publisher=publisher)
                    #indexData.apply_async((email,), publisher=publisher)

                    #if emailStats[0] == 10:
                    #	break
                    
                    #statistics
                    if emailStats[0] % 3000 == 0:
                        print emailStats[0]
            
            #if emailStats[0] == 10:
            #break
  
    except EnvironmentError:
        print 'I/O error'
    finally:
        publisher.close()
        connection.close()

    duration  = time.time() - start
   
    print "Number of emails: " + str(emailStats[0])
    print "Size of emails: " + str(emailStats[1])
    print "Duration:" + str(duration/60)

if __name__ == '__main__':
    main()

