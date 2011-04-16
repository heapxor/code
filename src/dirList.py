import os, sys, time

from os.path import join, getsize

from tasks import insertData
from tasks import checkData
from celery.exceptions import TimeLimitExceeded
from celery.utils import gen_unique_id
from celery.result import TaskSetResult
from celery import current_app
from celery.task.control import rate_limit

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

    start = time.time()



    try:
    	for root, dirs, files in os.walk('/big/mails'):
	    for name in files:
	        email = join(root,name)
	        
		if not 'envelope' in email:
		    size = getsize(email)
		    emailStats[0] += 1
		    emailStats[1] += size
		    #ret = 
		    #print email

		    #insertData.apply_async((email, ), publisher=publisher)
		    checkData.apply_async((email,), publisher=publisher)
		    #testTask.delay()
		    #tset.subtasks.append(ret)

	            #if emailStats[0] == 10:
		    #	break
                    
		    if emailStats[0] % 3000 == 0:
		        time.sleep(5)
		    	print emailStats[0]
            
	    #if emailStats[0] == 10:
	    #    break

  
  	#print("Total time: %r" % sum(tset.join(propagate=False)))
	#print([(r.status, r.result, r.traceback) for r in tset.subtasks])

	#print([r.traceback for r in tset.subtasks if r.failed()])
        #print("Tasks %d" % tset.completed_count())
	#print("Numb of compared emails : %r" % sum(tset.join(propagate=False)))

    except EnvironmentError:
    	print 'Read err'
    finally:
    	publisher.close()
	connection.close()

    duration  = time.time() - start
   
    print "number of e: " + str(emailStats[0])
    print "size of e: " + str(emailStats[1])
    print "duration:" + str(duration/60)

if __name__ == '__main__':
    main()

    #insertData.delay('/big/emailBackup/cassandra/568/cvut1129976380580531698')
    #insertData.delay('/big/emailBackup/cassandra/568/cvut1129989004280525575')

    
