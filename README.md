#HARBOR internal database structure using Hadoop
 
HARBOR performs three primary tasks:

1: It matches requests and replies: When the data includes both a request and reply with the same network 4-tuple with a timestamp that is within 10 seconds, it creates a stream of matched records which include both the request and reply.

2: It writes the output stream of matched request/reply pairs into a set of independent "Query Focused Datasets" (QFDs). In a QFD, a particular key (such as the source IP, destination IP, cookie, or username) is hashed using a cryptographic hash function. Then the lowest 2 bytes are used to select which file to write into, with the resulting file containing both the hash and the data.

This enables efficient searching for
All records associated with a username
All records associated with a tracking cookie
All records associated with a source IP or a destination IP
The particular QFD keys we will be using are:
cookie: Cookies included in user requests
srcIP: Source IP addresses
destIP: Destination IP addresses

3: It provides a second tool to create the TOTALFAIL database of Tor users (the torusers QFD). This tool first accesses the QFD which indexes matched users by source IP to get the users who are seen using a set of Tor exit nodes. Using any tracking cookies present in the resulting query, it accesses the tracking cookie QFD to get a list of all users, and then does a query for the usernames associated with those tracking cookies to create the TOTALFAIL QFD.

Author: Andy Chu
