import celery, sys
import hashlib
#sys.path.insert(0, '/usr/local/mailman/bin'


from emailReader import rawEmail
from celery import current_app
from celery.task import task
from celery.task.control import rate_limit
from emailParser import parseEmail
from eventlet import monkey_patch




monkey_patch()


@task(ignore_result=True)
#@task
def insertData(fileName):

    logger = insertData.get_logger()
    logger.info("Reading email %s" % fileName)
    logger.info("Task id %s" %  insertData.request.id)

    runTime = parseEmail(fileName)
    #print "Running time %s" % runTime
    #print fileName
    #return runTime


#@task(ignore_result=True, rate_limit="15/m")
@task(ignore_result=True)
def checkData(key):
    
    logger = insertData.get_logger()
    logger.info("Reading email %s" % key)
    logger.info("Task id %s" %  checkData.request.id)

    email = rawEmail(key)

    if email != 0: 

        f = open(key, 'r')
        sEmail = f.readlines()
        f.close()

        m = hashlib.sha1()
        m.update(''.join(sEmail))
        sHash = m.hexdigest()

        m = hashlib.sha1()
        m.update(email)
        dHash = m.hexdigest()
 
        if sHash != dHash:
            logger.info("[Error/EmailTest] Email corrupted.] < %s >" % key)
            path = '/big/testemails/' + key[key.rfind('/')+1:]
            f = open(path, 'w')
            f.write(email)
            f.close()
            #return "0"
        else:
            logger.info("[Email data test: OK]")
            #return "1"

