#!/usr/bin/python
# -*- coding: utf-8 -*-
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, \
    GlobalAllIndex
from boto.dynamodb2.table import Table


class db_instance:

    def __init__(self, name):
        self.name = name
        self.last_seq = 0
        self.db = Table(name,
                        connection=boto.dynamodb2.connect_to_region('us-west-2'
                        ))

        print 'Database instance: ' + self.name + ' connected!'



			