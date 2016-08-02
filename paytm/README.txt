*******************
Introduction
*******************

This challenge was completed using Python 2.7 and vanilla Apache Hadoop 2.7.2

Session duration chosen: 30 minutes


*******************
Tasks
*******************

* Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window. https://en.wikipedia.org/wiki/Session_(web_analytics)
    - This was achieved using the following mapper and reducer: sessionize_mapper.py, sessionize_reducer.py
    - Results: sessionized_sample.log

* Determine the average session time (Builds on results from sessionization):
    - This was achieved using the following mapper and reducer: avg_session_mapper.py, avg_session_reducer.py
    - Average session time: 6.10436747885 minutes
    - Results: average_session_duration.log

* Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session. (Builds on results from sessionization):
    - This was achieved using the following mapper and reducer: unique_url_mapper.py, unique_url_reducer.pys
    - Results: unique_urls_sample.log

* Determine session durations (Builds on results from sessionization):
    - This was achieved using the following mapper and reducer: session_duration_mapper.py, session_duration_reducer.py
    - Results: sessions_duration.log

* Find the most engaged users, ie the IPs with the longest session times (Builds on results from session durations)
    - This was achieved using the following mapper and reducer: top_user_mapper.py, top_user_reducer.py
    - Results: top_users.log

* IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions:
    - User ID (UUID) would be required to truly have unique users