#!/usr/bin/python
# -*- coding: utf-8 -*-

# DB Instance workers for code

# Standard libraries

import time
import json
import random
import argparse
import contextlib
import sys
import signal
import os.path
import argparse
import zmq
import heapq

import kazoo.exceptions

# Local modules

import gen_ports
import kazooclientlast

# Installed packages

import boto.sqs
import boto.sqs.message
from boto.sqs.message import Message

from db_instance import db_instance

from create import create
from delete import delete
from retrieve import retrieve
from add_activities import add_activities

AWS_REGION = 'us-west-2'
QUEUE_IN = 'ex6_in'
QUEUE_OUT = 'ex6_out'
MAX_WAIT_S = 20  # SQS sets max. of 20 s
DEFAULT_VIS_TIMEOUT_S = 60
TABLE = sys.argv[1]

BASE_INSTANCE_NAME = sys.argv[1]

# Names for ZooKeeper hierarchy

APP_DIR = '/DB-SUPERGS'
PUB_PORT = '/Pub'
SUB_PORTS = '/Sub'

current_db = sys.argv[1]

# print current_db

base_port = int(sys.argv[2])
zk_host = sys.argv[3]
sub_name = sys.argv[4]
global proxy_list
proxy_list = sys.argv[5]
sqs_in_name = sys.argv[6]
sqs_out_name = sys.argv[7]
db_list = sys.argv[8:]

shared = True
if shared:
    SEQUENCE_OBJECT = APP_DIR + '/SeqNum'
else:
    SEQUENCE_OBJECT = APP_DIR + '/SeqNum1'

DEFAULT_NAME = BASE_INSTANCE_NAME + '1'
BARRIER_NAME = '/Ready'

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

try:
    conn = boto.sqs.connect_to_region(AWS_REGION)
    if conn == None:
        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
        sys.exit(1)

    sqs_out = conn.create_queue(sqs_out_name)
except Exception, e:

    sys.stderr.write('Exception connecting to SQS\n')
    sys.stderr.write(str(e))
    sys.exit(1)


def get_ports():
    ''' Return the publish port and the list of subscribe ports '''

    # db_names = [BASE_INSTANCE_NAME+str(i) for i in range(1, 1+3)] #args.number_dbs
    # print db_names, args.proxy_list.split(',')

    proxies = []
    if proxy_list != '':
        proxies = proxy_list.split(',')
        print proxies
    else:
        proxies = []
    return gen_ports.gen_ports(base_port, db_list, proxies, sys.argv[1])


def setup_pub_sub(zmq_context, sub_to_name):
    ''' Set up the publish and subscribe connections '''

    global pub_socket
    global sub_sockets

    (pub_port, sub_ports) = get_ports()

    pub_socket = zmq.Context().socket(zmq.PUB)
    print 'instance ' + TABLE + ' binding on {1}'.format('data',
            pub_port)
    pub_socket.bind('tcp://*:{0}'.format(pub_port))
    sub_sockets = []
    for sub_port in sub_ports:
        sub_socket = zmq.Context().socket(zmq.SUB)
        sub_socket.setsockopt(zmq.SUBSCRIBE, '')
        print 'instance {0} connecting to {1} on {2}'.format(TABLE,
                sub_to_name, sub_port)
        sub_socket.connect('tcp://{0}:{1}'.format(sub_to_name,
                           sub_port))
        sub_sockets.append(sub_socket)


@contextlib.contextmanager
def zmqcm(zmq_context):
    global glob_zq
    glob_zq = zmq_context
    try:
        yield zmq_context
    finally:
        pass

        # The "0" argument destroys all pending messages
        # immediately without waiting for them to be delivered

        zmq_context.destroy(0)


@contextlib.contextmanager
def kzcl(kz):
    global glob_kz
    glob_kz = kz
    kz.start()
    try:
        yield kz
    finally:
        pass


        # kz.stop()
        # kz.close()

def signal_handler(signal, frame):
    print 'Close Signal Detected, Gracefully closing zookeeper connection and zeromq sockets!'
    glob_kz.stop()
    glob_kz.close()
    glob_zq.destroy(0)
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

global seq_num

with kzcl(kazooclientlast.KazooClientLast(hosts=zk_host)) as kz:
    with zmqcm(zmq.Context.instance()) as zmq_context:
        setup_pub_sub(zmq_context, sub_name)

        try:
            kz.create(path=SEQUENCE_OBJECT, value='0', makepath=True)
        except kazoo.exceptions.NodeExistsError, nee:
            pass
            kz.set(SEQUENCE_OBJECT, '0')  # Another instance has already created the node

                                         # or it is left over from prior runs
        # Wait for all DBs to be ready

        barrier_path = APP_DIR + BARRIER_NAME
        kz.ensure_path(barrier_path)

        try:
            b = kz.create(barrier_path + '/' + str(TABLE) + '_WORKER_'
                          + str('DB-SUPERGS'), ephemeral=True)
        except:
            print 'Barrier Already Created'
        while len(kz.get_children(barrier_path)) < 1:
            time.sleep(1)

        # print "Past rendezvous"

        seq_num = kz.Counter(SEQUENCE_OBJECT)

db_boto = db_instance(TABLE)
heap = []


def runOperation(zm_json, db_boto):
    if str(zm_json['request_type']) == str('retrieve'):
        print 'Execute retrieve on ' + str(TABLE)
        dyn_response = retrieve.do_retrieve(zm_json['id'],
                zm_json['name'], zm_json['activities'], db_boto)

    if str(zm_json['request_type']) == str('create'):
        print 'Execute create on ' + str(TABLE)
        dyn_response = create.do_create(zm_json['id'], zm_json['name'],
                zm_json['activities'], db_boto)

    if str(zm_json['request_type']) == str('add_activities'):
        print 'Execute add_activities on ' + str(TABLE)
        dyn_response = add_activities.do_add_activities(zm_json['id'],
                zm_json['activities'], db_boto)

    if str(zm_json['primary_instance']) == str(TABLE):
        m = Message()
        js = json.dumps(dyn_response)
        m.set_body(js)
        sqs_out.write(m)


zm_msg_list = []
while True:
    try:
        for i in range(0, len(sub_sockets)):
            zm_msg = ''
            try:
                zm_msg = sub_sockets[i].recv(flags=1)
                print zm_msg
            except:
                pass
            if len(zm_msg) > 5:
                print 'Current Sequence Number: ' \
                    + str(seq_num.last_set)
                print 'Recieving Operation from sub channel on instance: ' \
                    + str(TABLE)

                zm_json = json.loads(zm_msg)

                # Case when received from other worker

                if int(zm_json['seq_num']) - db_boto.last_seq == 1:
                    db_boto.last_seq += 1
                    print "Operate on current command as it's in correct order." \
                        + ' on instance ' + str(TABLE)
                    print zm_json
                    runOperation(zm_json, db_boto.db)

                    for i in range(0, len(heap)):
                        min_heap_json = heap[0]
                        min_seq_heap = min_heap_json[0]
                        if int(min_seq_heap[0]) - db_boto.last_seq == 1:
                            print 'Popping lowest seq from heap' \
                                + ' on instance ' + str(TABLE)
                            heapq.heappop(heap)
                            db_boto.last_seq += 1
                            print 'Operate on in order command from heap' \
                                + ' on instance ' + str(TABLE)
                            op_json = json.loads(min_seq_heap[1])
                            runOperation(op_json, db_boto.db)
                            print heap
                        else:
                            break
                elif int(zm_json['seq_num']) - db_boto.last_seq < 1:
                    print "do nothing, it's a repeat"
                else:
                    print 'Adding operation to heap data structure' \
                        + ' on instance ' + str(TABLE)
                    js_string = json.dumps(zm_json)
                    data = [(int(zm_json['seq_num']), js_string)]
                    heapq.heappush(heap, data)
                    print heap
    except:
        pass
    rs = sqs_in.get_messages()
    if len(rs) > 0:
        seq_num += 1
        m = rs[0]

        # Publish message that instance got from sqs_in

        seq_json = json.loads(m.get_body())
        seq_json['seq_num'] = seq_num.last_set
        seq_json['primary_instance'] = TABLE
        seq_json = json.dumps(seq_json)
        print str(TABLE) + ' got data from queue'
        print str(TABLE) + ' Publishing ' + str(seq_json)

        pub_socket.send_string(seq_json)

        # Case when received from other worker

        if seq_num.last_set - db_boto.last_seq == 1:
            db_boto.last_seq += 1
            print "Operate on current command as it's in correct order" \
                + 'on instance ' + str(TABLE)
            runOperation(json.loads(seq_json), db_boto.db)
            for i in range(0, len(heap)):
                min_heap_json = heap[0]
                min_seq_heap = min_heap_json[0]
                if int(min_seq_heap[0]) - db_boto.last_seq == 1:
                    print 'Popping lowest seq from heap' \
                        + ' on instance ' + str(TABLE)
                    heapq.heappop(heap)
                    db_boto.last_seq += 1
                    print 'Operate on in order command from heap' \
                        + ' on instance ' + str(TABLE)
                    op_json = json.loads(min_seq_heap[1])
                    runOperation(op_json, db_boto.db)
                else:
                    break
        elif int(json.loads(seq_json)['seq_num']) - db_boto.last_seq \
            < 1:

            print "Do nothing, it's a repeated message on instance " \
                + str(TABLE)
        else:
            print 'Adding operation to heap data structure' \
                + ' on instance ' + str(TABLE)
            js_string = seq_json
            data = [(int(seq_num.last_set), js_string)]
            heapq.heappush(heap, data)

        # Deleting message

        sqs_in.delete_message(m)

            