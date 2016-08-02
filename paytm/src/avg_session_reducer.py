#!/usr/bin/env python

from __future__ import division
import sys
import datetime

def reducer():
    curr_session_id = None
    prev_session_id = None
    count = 0
    timestamp_list = []
    duration_sum = 0.0
    for line in sys.stdin:
        data = line.strip().split('\t')
        if len(data) != 2:
        	continue

        curr_session_id, timestamp = data[0], data[1]
        timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%X.%f')

        if prev_session_id and prev_session_id != curr_session_id:
            # Calculate diff between first timestamp and last timestamp for that session (Session duration)
            if len(timestamp_list) >= 2:
                timestamp_list.sort()
            	diff = timestamp_list[-1] - timestamp_list[0]
            	# Compute and add duration in minutes
            	duration_sum += diff.total_seconds() / 60
            	count += 1
            timestamp_list = []

        timestamp_list.append(timestamp)
        prev_session_id = curr_session_id
    else:
    	if len(timestamp_list) >= 2:
            diff = timestamp_list[-1] - timestamp_list[0]
            # Compute and add duration in minutes
            duration_sum += diff.total_seconds() / 60
            count += 1
    	if count > 0:
    	   print "{0}".format(duration_sum/count)

if __name__ == "__main__":
    reducer()
