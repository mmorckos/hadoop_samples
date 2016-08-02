#!/usr/bin/env python

from __future__ import division
import sys
import datetime

# 30-minute session in seconds
SESSION_DURATION = 30

'''
User data class
'''
class UserData:
    def __init__(self, timestamp, raw_timestamp, url):
        self.timestamp = timestamp
        self.raw_timestamp = raw_timestamp
        self.url = url

'''
Aggregate timestamps per user
'''
def reducer(users):
    curr_user_ip = None
    prev_user_ip = None
    curr_user_data = None
    data_list = []
    for line in sys.stdin:
        data = line.strip().split('\t')
        if len(data) != 3:
            continue

        curr_user_ip, raw_timestamp, url = data[0], data[1], data[2]
        timestamp = datetime.datetime.strptime(raw_timestamp, '%Y-%m-%dT%X.%f')
        data_list.append(UserData(timestamp, raw_timestamp, url))

        if prev_user_ip and prev_user_ip != curr_user_ip:
            if prev_user_ip in users:
                users[prev_user_ip].extend(data_list)
            else:
                users[prev_user_ip] = data_list
            data_list = []
        prev_user_ip = curr_user_ip

    if curr_user_ip in users:
        users[curr_user_ip].extend(data_list)
    else:
        users[curr_user_ip] = data_list

'''
Compute unique sessions for users
'''
def compute_sessions(users):
    for key in users:
        # Sort timestamps for user
        users[key].sort(key=lambda x : x.timestamp)
        prev_timestamp = None
        prev_session_id = None
        # Loop through all sorted timestamps for user and split them into sessions
        for data in users[key]:
            session_id = None
            timestamp = data.timestamp
            diff = timestamp
            minutes = SESSION_DURATION + 1
            if prev_timestamp is not None:
                diff = timestamp - prev_timestamp
                minutes = diff.total_seconds() / 60
            if minutes > SESSION_DURATION:
                session_id = key + '_' + data.raw_timestamp
            else:
                session_id = prev_session_id
            prev_timestamp = timestamp
            prev_session_id = session_id
            # Output session id, timestamp, and url
            print "{0}\t{1}\t{2}".format(session_id, data.raw_timestamp, data.url)

def driver():
    users = {}
    # Run reducer function
    reducer(users)
    # Compute sessions for all users
    compute_sessions(users)

if __name__ == "__main__":
    driver()
