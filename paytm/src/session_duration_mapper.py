#!/usr/bin/env python

import sys
import csv

def mapper():
    for row in sys.stdin:
        data = next(csv.reader([row], delimiter = '\t'))
        # Skip unformatted records
        if len(data) != 3:
            continue
        # Print-out session id, timestamp
        print "{0}\t{1}".format(data[0], data[1])

if __name__ == "__main__":
    mapper()
