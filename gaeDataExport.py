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

def getDumpDir():
    if len(sys.argv) < 2:
        print 'Error: need to pass in location of backup directory'
        sys.exit(1)
    dumpDir = sys.argv[1]
    print 'using dumpDir:', dumpDir
    return dumpDir

def extractTableName(path):
    match = re.search('[A-Za-z0-9]+(?=/[0-9]+)', path)
    if match:
        return match.group(0)
    print 'error: could not find table name in path: ', path
    sys.exit(1)

def listFiles(dumpDir):
    for root, dirs, files in os.walk(dumpDir):
        valid_files = [f for f in files if not f.endswith('info')]
        if len(valid_files) != 0:
            table_name = extractTableName(root)
            print 'files in ' + table_name + ': ' + ",".join(valid_files)

def main():
    dumpDir = getDumpDir()
    listFiles(dumpDir)

if __name__ == "__main__":
    main()

