#!/usr/bin/env python

#
# Reads data from a Google App Engine backup and exports it to a CSV file
# to stop all process
# kill -TERM -$(pgrep -o python)
#

import sys
import os
from os.path import join
import re
import csv
import traceback
import time
import copy
from multiprocessing import Pool
from multiprocessing import cpu_count
from itertools import izip
from itertools import repeat

# import Google App Engine api
sys.path.append('/usr/local/google_appengine')
from google.appengine.api.files import records
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore
from google.appengine.api import datastore_types
from google.appengine.ext.db import BadKeyError

key_regex = 'ah[^\s]{20,}'

def getDirs():
    if len(sys.argv) < 3:
        print 'Error: need to pass in location of backup directory and output directory'
        sys.exit(1)
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    print 'using input directory:', input_dir
    print 'using output directory:', output_dir
    print ''
    return input_dir, output_dir

def extractTableName(path):
    match = re.search('[A-Za-z0-9]+(?=/[0-9]+)', path)
    if match:
        return match.group(0)
    print 'error: could not find table name in path: ', path
    sys.exit(1)

def listFiles(directory):
    list = []
    for root, dirs, files in os.walk(directory):
        valid_files = [f for f in files if not f.endswith('info')]
        if len(valid_files) != 0:
            table_name = extractTableName(root)
            list.append((root, table_name, valid_files))
    return list

def decodeStringKey(value):
    # first try to decode the whole string as one key
    try:
        key = datastore_types.Key(value)
        return encode(key)
    except BadKeyError as e:
        # check for the case of two keys split by a '_'
        if re.search('^' + key_regex + '_' + key_regex + '$', value):
            keys = value.split('_ah')
            return encode(keys[0]) + '_' + encode('ah' + keys[1])
        # check for the case of one key and one string split by a '_'
        if re.search('^' + key_regex + '_[^\s]+$', value):
            strings = value.split('_')
            # one key and one string
            if len(strings) == 2:
                return encode(strings[0]) + '_' + encode(strings[1])
            # one key (which includes a '_') and one string
            elif len(strings) == 3:
                return encode('_'.join(strings[0:2])) + '_' + encode(strings[2])
        raise Exception('bad key string: ' + str(value), e)

def encode(value):
    if value is None:
        return ''

    # convert unicode to simpler string
    if type(value) is unicode or type(value) is datastore_types.Text:
        value = value.encode('utf-8', errors = 'replace')

    if type(value) is datastore_types.Key:
        return encode(value.kind()) + '@' + encode(value.id_or_name())

    # if it's a string representation of a Key, recursively encode it
    if type(value) is str and re.search('^' + key_regex + '$', value):
        return decodeStringKey(value)

    # simple case
    return str(value)

def parseHeaderFields(entity):
    return [field for field in entity]

def entity2csvRow(entity_fields, entity):
    row = []
    # always save entity's key first
    row.append(entity.key())
    for field in entity_fields:
        value = None
        if field in entity:
            value = entity[field]
        row.append(value)
    return row

class TimeCapture(object):
    def __init__(self, init_time):
        self.init_time = init_time
        self.start_time = init_time
        self.end_time = None
        self.run_time = None
        self.total_time = None
        self.count = None
        self.ms_per_obj = None

    def start(self):
        self.start_time = time.time()

    def end(self, count):
        self.end_time = time.time()
        self.run_time = self.end_time - self.start_time
        self.count = count
        self.ms_per_obj = self.run_time * 1000 / count
        self.total_time = self.end_time - self.init_time


def process(output_dir, time_capture, table_tuple, writeFn):
    root = table_tuple[0]
    table_name = table_tuple[1]
    filenames = table_tuple[2]
    header_list = None

    print 'Converting ' + table_name + ' to a CSV file...'
    time_capture.start()

    count = 0
    # open file for writing
    with open(join(output_dir, table_name + '.csv'), 'w') as write_file:
        write_file = csv.writer(write_file)
        # read files, process, and write
        #print 'filenames',filenames
        #print "______________________ extract for ", table_name, ' ______________'
        header_list = []
        display_header_list = ['key']
        for filename in filenames:
            path = join(root, filename)
            with open(path, 'r') as raw_file:
                reader1 = records.RecordsReader(raw_file)
                for record in reader1:
                    entity_proto1 = entity_pb.EntityProto(contents=record)
                    entity1 = datastore.Entity.FromPb(entity_proto1)
                    headers = parseHeaderFields(entity1)
                    for header in headers:
                        if len(headers) > len(header_list):
                            header_list = headers
                            #print 'header_list=',header_list
        display_header_list.extend(header_list)
        #print "header_list", header_list
        writeFn(write_file, None, display_header_list)
        for filename in filenames:
            path = join(root, filename)
            with open(path, 'r') as raw_file:
                reader = records.RecordsReader(raw_file)
                for record in reader:
                    entity_proto = entity_pb.EntityProto(contents=record)
                    entity = datastore.Entity.FromPb(entity_proto)
                    if header_list is None:
                        header_list = parseHeaderFields(entity)
                        display_header_list = ['key']
                        display_header_list.extend(header_list)
                        writeFn(write_file, entity, display_header_list)
                    csv_row = entity2csvRow(header_list, entity)
                    writeFn(write_file, entity, csv_row)
                    count+=1
    time_capture.end(count)
    print '    ...converted {count:d} objects of type {obj_type} in {run_time:.2f} seconds | {ms_per_obj:.2f} ms/obj | total time = {total_time:.2f} seconds'.format(count=count, obj_type=table_name, run_time=time_capture.run_time, ms_per_obj=time_capture.ms_per_obj, total_time=time_capture.total_time)
    return time_capture

def multiprocess(input):
    return process(*input)

def write(write_file, entity, row):
    row = [encode(r) for r in row]
    try:
        write_file.writerow(row)
    except:
        traceback.print_exc()
        print 'Row:'
        print row
        print 'Entity:'
        print entity
        sys.exit(1)

def displayResults(results, total_time):
    types = len(results)
    total_count = sum([result.count for result in results])
    total_run_time = sum([result.run_time for result in results])
    total_count = total_count or 1
    avg_ms_per_obj = total_run_time * 1000 / total_count
    print 'Converted {types:d} types with a total of {count:d} objects | total running time = {run:.2f} seconds | {rate:.2f} ms/obj | total time = {total:.2f} seconds'.format(types=types, count=total_count, run=total_run_time, rate=avg_ms_per_obj, total=total_time)

def main():
    input_dir, output_dir = getDirs()
    table_list = listFiles(input_dir)
    print "found ",table_list

    concurrency = cpu_count()
    print 'Using {0:d} Processes'.format(concurrency)
    pool = Pool(concurrency)

    # perform the passed in write action (function) for each csv row
    time_capture = TimeCapture(time.time())
    results = pool.map(
        multiprocess,
        izip(repeat(output_dir),
            [copy.deepcopy(time_capture) for i in range(len(table_list))],
            table_list,
            repeat(write)))
    time_capture.end(1)
   
    pool.close()
    pool.join()

    print 'Finished Successfully!'
    displayResults(results, time_capture.total_time)

if __name__ == "__main__":
    main()
