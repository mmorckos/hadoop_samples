#!/usr/bin/env python

from __future__ import division
import sys
import datetime

def reducer():
    for line in sys.stdin:
        data = line.strip().split('\t')
        if len(data) != 2:
            continue

        duration, ip = data[0], data[1]
        print "{0}\t{1}".format(ip, duration)

if __name__ == "__main__":
    reducer()
