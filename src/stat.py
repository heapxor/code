from tasks import insertData
from celery.exceptions import TimeLimitExceeded
from celery.utils import gen_unique_id
from celery.result import TaskSetResult
from celery import current_app
import ttest
from celery.task import chord
from comparatorTest import dataTest
"""
callback = compareE.subtask()
h = [insertE.subtask(), readE.subtask()]
res = chord(h)(callback)
res.get()

"""

#status('cemail')

dataTest('email/email2')

