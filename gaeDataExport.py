#!/usr/bin/env python

#
# Reads data from a Google App Engine backup and exports it to a CSV file
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

def encode(val):
    if type(val) is unicode or type(val) is datastore_types.Text:
        return val.encode('utf-8', errors = 'replace')
    return str(val)

def parseHeaderFields(entity):
    return [field for field in entity]

def entity2csvRow(entity_fields, entity):
    row = []
    for field in entity_fields:
        if field in entity:
            value = entity[field] or ''
        else:
            value = ''
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
        for filename in filenames:
            path = join(root, filename)
            with open(path, 'r') as raw_file:
                reader = records.RecordsReader(raw_file)
                for record in reader:
                    entity_proto = entity_pb.EntityProto(contents=record)
                    entity = datastore.Entity.FromPb(entity_proto)
                    if header_list is None:
                        header_list = parseHeaderFields(entity)
                        writeFn(write_file, entity, header_list)
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
    avg_ms_per_obj = total_run_time * 1000 / total_count
    print 'Converted {types:d} types with a total of {count:d} objects | total running time = {run:.2f} seconds | {rate:.2f} ms/obj | total time = {total:.2f} seconds'.format(types=types, count=total_count, run=total_run_time, rate=avg_ms_per_obj, total=total_time)

def main():
    input_dir, output_dir = getDirs()
    table_list = listFiles(input_dir)

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
