#!/usr/bin/env python

import sys
import csv

def mapper():
    for row in sys.stdin:
        data = next(csv.reader([row], delimiter = '\t'))
        # Skip unformatted records
        if len(data) != 2:
            continue
        # Print-out session duration, user ip
        print "{0}\t{1}".format(data[1], data[0].split('_')[0])

if __name__ == "__main__":
    mapper()
