#!/usr/bin/python
# -*- coding: utf-8 -*-

# Library packages
# import json

import os
import re
import sys
import json
import os.path
import argparse

# Installed packages

import boto.sqs
import signal

from bottle import route, run, request, response, default_app

AWS_REGION = 'us-west-2'
QUEUE_OUT = 'ex6_out'
MAX_WAIT_S = 20  # SQS sets max. of 20 s
PORT = 8081

sqs_out_name = sys.argv[1]


def signal_handler(signal, frame):
    print 'Close Signal Detected, close parent proess then gracefully close rest!'
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

try:
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)

    q = conn.create_queue(sqs_out_name)
except Exception, e:

    sys.stderr.write('Exception connecting to SQS\n')
    sys.stderr.write(str(e))
    sys.exit(1)


@route('/')
def app():

    # m = {'id': 0, 'f': 10, 's': 10, 'actual_s': 5}

    msg = None
    wq = q.get_messages()
    if len(wq) > 0:
        m = wq[0]
        msg = m.get_body()
        msg = json.loads(msg)
        q.delete_message(m)
    if msg == None:
        response.status = 202
        json_api_spec = {}
        json_full_spec = {}
        json_full_spec['type'] = 'Notification'
        json_full_spec['msg'] = 'Queue Empty'
        json_api_spec['data'] = json_full_spec
        return json.dumps(json_api_spec)
    else:
        resp = msg
        return resp

app = default_app()
run(app, host='localhost', port=PORT)

            