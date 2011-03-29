import os, sys, time

from os.path import join, getsize

from tasks import insertData
from celery.exceptions import TimeLimitExceeded
from celery.utils import gen_unique_id
from celery.result import TaskSetResult
from celery import current_app


"""
		     try:
			result += ret.get()
		     except TimeLimitExceeded:
			result += 0
"""
                           
def main():	
    emailStats = [0, 0]
 
    tset = TaskSetResult(gen_unique_id(), [])
    connection = current_app.broker_connection()
    publisher = current_app.amqp.TaskPublisher(connection)

    try:
    	for root, dirs, files in os.walk('/big/emailBackup/'):
	    for name in files:
	        email = join(root,name)
	        
		if not 'envelope' in email:
		    size = getsize(email)
		    emailStats[0] += 1
		    emailStats[1] += size
		    ret = insertData.apply_async((email, ), publisher=publisher)
		    tset.subtasks.append(ret)

	            if emailStats[0] == 3300:
		    	break

	    if emailStats[0] == 3300:
	        break
   	
	time.sleep(20)
        print("Total time: %r" % sum(tset.join(propagate=False)))
	#print tset.join()

	#print("Tasks %d" % tset.completed_count())
    except EnvironmentError:
    	print 'read err'
    finally:
    	publisher.close()
	connection.close()

    
   
    print "number of e: " + str(emailStats[0])
    print "size of e: " + str(emailStats[1])

if __name__ == '__main__':
    main()

    #insertData.delay('/big/emailBackup/cassandra/568/cvut1129976380580531698')
    #insertData.delay('/big/emailBackup/cassandra/568/cvut1129989004280525575')

    
