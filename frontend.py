#!/usr/bin/python
# -*- coding: utf-8 -*-

# Standard library packages

import sys
import json
import time
import signal
import os.path
import argparse
import contextlib
import os
import re
import boto.dynamodb2
import subprocess

from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, \
    GlobalAllIndex
from boto.dynamodb2.table import Table

import boto.sqs
import boto.sqs.message
from boto.sqs.message import Message

from create import create
from delete import delete
from retrieve import retrieve
from add_activities import add_activities

# Installed packages

import zmq

import kazoo.exceptions
from bottle import route, run, request, response, abort, default_app, \
    get, post

# Local modules

import gen_ports
import kazooclientlast

REQ_ID_FILE = 'reqid.txt'


def build_parser():
    ''' Define parser for command-line arguments '''

    parser = \
        argparse.ArgumentParser(description='Web server demonstrating final project technologies'
                                )
    parser.add_argument('zkhost',
                        help='ZooKeeper host string (name:port or IP:port, with port defaulting to 2181)'
                        )
    parser.add_argument('web_port', type=int,
                        help='Web server port number', nargs='?',
                        default=8080)
    parser.add_argument('name', help='Name of this instance', nargs='?'
                        , default='DB')
    parser.add_argument('number_dbs', type=int,
                        help='Number of database instances', nargs='?',
                        default=1)
    parser.add_argument('base_port', type=int,
                        help='Base port for publish/subscribe',
                        nargs='?', default=7777)
    parser.add_argument('sub_to_name',
                        help='Name of system to subscribe to', nargs='?'
                        , default='localhost')
    parser.add_argument('proxy_list',
                        help='List of instances to proxy, if any (comma-separated)'
                        , nargs='?', default='')
    parser.add_argument('sqs_in_name', help='Name of sqs in queue',
                        nargs='?', default='sqs_in')
    parser.add_argument('sqs_out_name', help='Name of sqs out queue',
                        nargs='?', default='sqs_out')
    parser.add_argument('read_capacity', type=int,
                        help='database table read capacity', nargs='?',
                        default=10)
    parser.add_argument('write_capacity', type=int,
                        help='database table write capacity', nargs='?'
                        , default=10)
    parser.add_argument('shell_db_names', help='db_names', nargs='?',
                        default='')
    return parser


# Get the command-line arguments

parser = build_parser()
args = parser.parse_args()

SHELL_DB_NAMES = str(args.shell_db_names)
SHELL_DB_NAMES_LIST = args.shell_db_names.split(',')
WEB_PORT = args.web_port

# Instance naming

BASE_INSTANCE_NAME = args.name

# Names for ZooKeeper hierarchy

PUB_PORT = '/Pub'
SUB_PORTS = '/Sub'
BARRIER_NAME = '/Ready'

# Publish and subscribe constants

SUB_TO_NAME = args.sub_to_name
SUB_LIST = []
SUB_LIST.append(SUB_TO_NAME)

# proxy_list

PROXY = args.proxy_list
PROXY_LIST = []
PROXY_LIST.append(PROXY)

BASE_PORT = args.base_port
ZK_HOST = args.zkhost
ZK_HOST_LIST = []
ZK_HOST_LIST.append(ZK_HOST)

# sqs_in queue name

sqs_in_name = args.sqs_in_name

# sqs_out queue name (backend.py)

sqs_out_name = args.sqs_out_name

# database table read/write capacity

db_read_cap = args.read_capacity
db_write_cap = args.write_capacity

name_list = []
name_string = ''

NAME_STRING_LIST = []

NAME_STRING_LIST.append(SHELL_DB_NAMES)

AWS_REGION = 'us-west-2'


def signal_handler(signal, frame):
    print 'Close Signal Detected, close parent proess then gracefully close rest!'
    print pro_proxy_list

        # I need this code as proxy.py has threads and it wont detect my signals, however since I'm using subprocesses and my goal is just to kill them, THIS WORKS GOOD

    for i in range(0, len(pro_proxy_list)):
        pro_proxy_list[i].kill()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def createDynamoObject(name):
    try:
        users = Table.create(name, schema=[HashKey('id')],
                             throughput={'read': db_read_cap,
                             'write': db_write_cap},
                             global_indexes=[GlobalAllIndex('EverythingIndex'
                             , parts=[HashKey('name')])],
                             connection=boto.dynamodb2.connect_to_region(AWS_REGION))
    except:
        users = Table(name,
                      connection=boto.dynamodb2.connect_to_region('us-west-2'
                      ))
        print "1) Table 'data' already created for table: " + name

  # On first Run this wont insert data because of delay to create table on aws server side.

    try:
        users.put_item(data={
            'id': '3',
            'type': 'person',
            'name': 'dummy',
            'activities': ['activity one'],
            })
    except:
        print '2) Dummy Data already added for tabe: ' + name
    return users


print 'Starting Fault Tolerant System....\n'

print "Creating DB's for Testing Purposes...\n"

db_ref_list = []
for i in range(0, len(SHELL_DB_NAMES_LIST)):
    db_ref_list.append(createDynamoObject(SHELL_DB_NAMES_LIST[i]))

print name_string

print 'Connecting to SQS_IN...\n'

try:
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)

    sqs_in = conn.create_queue(sqs_in_name)
except Exception, e:

    sys.stderr.write('Exception connecting to SQS\n')
    sys.stderr.write(str(e))
    sys.exit(1)

print 'Starting rest of stuff...\n'


def main():
    global args
    global table
    global seq_num
    global req_file
    global rq_count
    global request_count

    print 'Starting DB Instances***\n'

    # create database

    cmd = './db_worker.py'
    cmd_proxy = './proxy.py'
    port = []
    port.append(str(BASE_PORT))

    global pro_proxy_list
    pro_proxy_list = []

    for i in range(1, 1 + len(SHELL_DB_NAMES_LIST)):
        db_names = SHELL_DB_NAMES_LIST[i - 1]
        db_name_list = []
        db_name_list.append(db_names)
        pro = subprocess.Popen([cmd, db_names] + port + ZK_HOST_LIST
                               + SUB_LIST + PROXY_LIST + [sqs_in_name,
                               sqs_out_name] + SHELL_DB_NAMES_LIST)

        if db_names in PROXY:
            print 'Starting proxy server for instance: ' + str(db_names)
            pro_proxy = subprocess.Popen([cmd_proxy, str(BASE_PORT)]
                    + NAME_STRING_LIST + PROXY_LIST + db_name_list)
            pro_proxy_list.append(pro_proxy)

    # Initialize request id from durable storage

    if not os.path.isfile(REQ_ID_FILE):
        with open(REQ_ID_FILE, 'w', 0) as req_file:
            req_file.write('0\n')

    try:
        req_file = open(REQ_ID_FILE, 'r+', 0)
        request_count = int(req_file.readline())
        print 'Current Request Count: ' + str(request_count)
        rq_count = request_count
    except IOError, exc:
        sys.stderr.write("Exception reading request id file '{0}'\n".format(REQ_ID_FILE))
        sys.stderr.write(exc)
        sys.exit(1)
    print 'Starting Backend Server***\n'
    cmd = './backend.py'
    pro = subprocess.Popen([cmd, sqs_out_name])
    app = default_app()
    run(app, host='localhost', port=args.web_port)


@route('/retrieve')
def retrieve_route():
    json_api_spec = {}
    rq_id = request.query.id
    rq_name = request.query.name
    rq_activities = request.query.activities
    global request_count
    print '\n'
    request_count += 1
    dict_sqs = {
        'id': rq_id,
        'name': rq_name,
        'activities': rq_activities,
        'request_type': 'retrieve',
        'rq_count': request_count,
        }
    m = Message()
    js = json.dumps(dict_sqs)
    m.set_body(js)
    sqs_in.write(m)
    json_ret = \
        json.dumps('{"data": {"type": "Notification","msg": "Accepted"}}'
                   )
    json_full_spec = {}
    json_full_spec['type'] = 'Notification'
    json_full_spec['msg'] = 'Accepted'
    json_api_spec['data'] = json_full_spec
    return json_api_spec


@route('/create')
def parse_create():
    json_api_spec = {}
    rq_id = request.query.id
    rq_name = request.query.name
    rq_activities = request.query.activities
    global request_count
    print '\n'
    request_count += 1
    dict_sqs = {
        'id': rq_id,
        'name': rq_name,
        'activities': rq_activities,
        'request_type': 'create',
        'rq_count': request_count,
        }
    m = Message()
    js = json.dumps(dict_sqs)
    m.set_body(js)
    sqs_in.write(m)
    json_ret = \
        json.dumps('{"data": {"type": "Notification","msg": "Accepted"}}'
                   )
    json_full_spec = {}
    json_full_spec['type'] = 'Notification'
    json_full_spec['msg'] = 'Accepted'
    json_api_spec['data'] = json_full_spec
    return json_api_spec


@route('/delete')
def parse_delete():
    rq_id = request.query.id
    rq_name = request.query.name
    rq_activities = request.query.activities
    json_full_spec = {}
    json_full_spec['type'] = 'Notification'
    json_full_spec['msg'] = 'Accepted'
    json_api_spec['data'] = json_full_spec
    return json_api_spec


@route('/add_activities')
def parse_add_activities():
    json_api_spec = {}
    rq_id = request.query.id
    rq_name = request.query.name
    rq_activities = request.query.activities
    global request_count
    print '\n'
    request_count += 1
    dict_sqs = {
        'id': rq_id,
        'name': rq_name,
        'activities': rq_activities,
        'request_type': 'add_activities',
        'rq_count': request_count,
        }
    m = Message()
    js = json.dumps(dict_sqs)
    m.set_body(js)
    sqs_in.write(m)
    json_ret = \
        json.dumps('{"data": {"type": "Notification","msg": "Accepted"}}'
                   )
    json_full_spec = {}
    json_full_spec['type'] = 'Notification'
    json_full_spec['msg'] = 'Accepted'
    json_api_spec['data'] = json_full_spec
    return json_api_spec

# Standard Python shmyntax for the main file in an application

if __name__ == '__main__':
    main()

      