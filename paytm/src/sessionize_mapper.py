#!/usr/bin/env python

import sys
import csv

def mapper():
    for row in sys.stdin:
        data = next(csv.reader([row], delimiter = ' '))
        # Skip unformatted records
        if len(data) != 15:
            continue
        # Skip record if user IP address, time, or URL is missing or malformed
        if data[0] == '-' or data[2] == '-' or data[11] == '-':
            continue
        timestamp = data[0].strip('Z')
        user_ip = data[2].split(':')[0]
        url = data[11].split(' ')[1]
        # Print-out timestamp, user IP, and URL
        print "{0}\t{1}\t{2}".format(user_ip, timestamp, url)

if __name__ == "__main__":
    mapper()
