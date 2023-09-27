import json
import requests
import sys
import time

# README
# How to use this script:
# This script will chunk a file and send it to the Mezmo Telemetry Pipeline endpoint for ingestion.
# This can be useful for cases you wish to test sending data from an endpoint directly or for
# real world simulation of a deployed pipeline.
#
# Edit the parameters below fo configure the file and then run it. You do not need to change any 
# values other than the configuration information at the top if you wish to use the default settings.

# File and configuration information goes here. See the Mezmo docs regarding the HTTP source
# if you need more information here: https://docs.mezmo.com/telemetry-pipelines/http-source 
api_key = "<INSERT KEY HERE>"
api_url = "<YOUR PIPELINE URL>"
my_file = "<YOUR LOCAL FILE>"

# How many times would you like to run? More times = more of the same data
# Use loops to continuously start over and feed the data in if you want to test longer
loops = 1

# Desired chunk size, 1000 events would be standard
chunk, chunk_size = [], 1000
max_size = 19500000 # maximum request size in bytes, 1950000 recommended, max payload is 2000000

# Set the timer between requests in seconds, use 0 for none
s = 1

# For sending the data chunks to the pipeline HTTP endpoint
def use_requests(chunk):
    response = requests.post(api_url, headers={'Authorization': api_key}, data=json.dumps(chunk))
    json_response = json.loads(response.text)
    print(json_response)
    time.sleep(s) # set how long to wait between requests
    return

def open_file(filename):
    reader = open(filename, mode='r')
    total_size = 0
    for i, line in enumerate(reader):
        size = sys.getsizeof(line)
        total_size = total_size + size
        if (i % chunk_size == 0 and i > 0):
            use_requests(chunk)
            del chunk[:]  # or: chunk = []
            total_size = size
            chunk.append(line)
        if line == '\n': # skip blank lines
            continue
        if (total_size > max_size): # before request size limit
            use_requests(chunk)
            del chunk[:]  # or: chunk = []
            total_size = size
            chunk.append(line)
        else:
            chunk.append(line)
    reader.close
    print("Loop " + str(x) + " " + filename)


# Set the counter for loops
x = 0

# Iterate over the file multiple times as specified in loops
while x < loops:
    open_file(my_file)
    x = x+1
