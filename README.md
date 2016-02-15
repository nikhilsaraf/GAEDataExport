# GAEDataExport
Export data from Google App Engine's datastore backup to a CSV file

# Usage
gaeDataExport.py /path/to/backup/directory /path/to/existing/output/directory

# How it works
Finds all the backup files and the associated collection names using the format used for GAE backups<br/>
Reads the backup files using the Google Apis and converts it to a CSV file named after the collection name

# Assumptions
This assumes that the first item read for each collection contains the superset of the fields in the collection. It then extracts only these fields from all the other documents in the collection. This can be improved.
