#!/usr/bin/env python

import sys
import csv

def mapper():
    for row in sys.stdin:
        data = next(csv.reader([row], delimiter = '\t'))
        # Skip unformatted records
        if len(data) != 3:
            continue
        # Print-out session id, url
        print "{0}\t{1}".format(data[0], data[2])

if __name__ == "__main__":
    mapper()
