import google.cloud.logging
import logging
import os

from flask import request

if not os.environ.get('RUN_LOCALLY'):
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    client = google.cloud.logging.Client()
    client.setup_logging()              
    logger = client.logger('scraper')
else:
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    logger = logging.Logger('scraper')

def logText(text):
    if not os.environ.get('RUN_LOCALLY'):
        logger.log_text(text)
    else:
       print(text)
    '''
    if not os.environ.get('RUN_LOCALLY'):
        options = {
        }
        trace_id = request.headers.get('X-Cloud-Trace-Context', 'no_trace_id').split('/')[0]
        if trace_id:
            options['trace'] = "projects/{}/traces/{}".format(os.getenv('GOOGLE_CLOUD_PROJECT'), trace_id)
    
        queueName = request.headers.get('X-CloudTasks-QueueName')
        if queueName:
            if not 'labels' in options:
                options['labels'] = {}
            options['labels']['queue_id'] = queueName

        taskName = request.headers.get('X-CloudTasks-TaskName')
        if taskName:
            if not 'jsonPayload' in options:
                options['jsonPayload'] = {}
            options['jsonPayload']['task'] = taskName

        logger.log_text(text, options)
    else:
        print(text)
    '''
