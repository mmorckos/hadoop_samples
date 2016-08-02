#!/usr/bin/env python

from __future__ import division
import sys

def reducer():
    curr_session_id = None
    prev_session_id = None
    url_set = set() 
    for line in sys.stdin:
        data = line.strip().split('\t')
        if len(data) != 2:
        	continue

        curr_session_id, url = data[0], data[1]

        if prev_session_id and prev_session_id != curr_session_id:
            # Print session id and url (All urls are unique per session)
            for item in url_set:
                print "{0}\t{1}".format(prev_session_id, item)
            url_set.clear()

        url_set.add(url)
        prev_session_id = curr_session_id
    else:
        for item in url_set:
            print "{0}\t{1}".format(prev_session_id, item)

if __name__ == "__main__":
    reducer()
