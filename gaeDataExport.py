#!/usr/bin/env python

#
# Reads data from a Google App Engine backup and exports it to a CSV file
#

import sys
import os
from os.path import join
import re

# import Google App Engine api
sys.path.append('/usr/local/google_appengine')
from google.appengine.api.files import records
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore

def getDirs():
    if len(sys.argv) < 3:
        print 'Error: need to pass in location of backup directory and output directory'
        sys.exit(1)
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    print 'using input directory:', input_dir
    print 'using output directory:', output_dir
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
            #print 'files in ' + table_name + ': ' + ",".join(valid_files)
            list.append((root, table_name, valid_files))
    return list

def parseHeaderFields(entity):
    return [field for field in entity]

def entity2csvRow(entity_fields, entity):
    row = []
    for field in entity_fields:
        value = entity[field]
        if value is None:
            value = ''
        row.append(str(value))
    return ",".join(row)

def process(output_dir, table_tuple, writeFn):
    root = table_tuple[0]
    table_name = table_tuple[1]
    filenames = table_tuple[2]
    header_list = None

    # open file for writing
    with open(join(output_dir, table_name), 'w') as write_file:
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
                        writeFn(write_file, ",".join(header_list))
                    csv_row = entity2csvRow(header_list, entity)
                    writeFn(write_file, csv_row)

def write(write_file, row):
    print 'writing row:', row
    write_file.write(row)
    write_file.write('\n')

def main():
    input_dir, output_dir = getDirs()
    table_list = listFiles(input_dir)
    # perform the passed in write action (function) for each csv row
    for table_tuple in table_list:
        process(output_dir, table_tuple, write)

if __name__ == "__main__":
    main()
