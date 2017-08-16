#!/usr/bin/python

import sys
import re
import csv

csvreader = csv.reader(sys.stdin, delimiter=',', quotechar='"')
field_names = csvreader.next()

for row in csvreader:

    if len(row) != len(field_names):
        print("error\t{0} entries in row instead of {1}: {2}".format(len(row), len(field_names), row))

    for i in range(len(row)):
        print(field_names[i] + '\t' + str(row[i]))    
