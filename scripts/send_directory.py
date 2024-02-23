import json
import sys
import requests
import os
import gzip
import time

# Put the file into the same directory as the script to run or specify a different path in the file variable
# If using a whole directory, see below

# Open the file directory specified if you want many files

# Desired chunk size, 1000 events would be standard
chunk, chunksize = [], 1000
max_size = 19500000 # maximum request size in bytes, 1950000 recommended, max payload is 2000000

# Credentials
api_key = ''
api_url = ''

# Directory to send
directory_string = ''

# Set some initial values
directory = os.fsencode(directory_string)
total_lines = 0
total_size = 0
total_files = 0
request_size = 0

# For sending the data chunks to the pipeline HTTP endpoint
def use_requests(chunk):

    # send it!
    response = requests.post(api_url, headers={'Authorization': api_key}, data=json.dumps(chunk))
    
    # calculate size
    method_len = len(response.request.method)
    url_len = len(response.request.url)
    headers_len = len('\r\n'.join('{}{}'.format(k, v) for k, v in response.request.headers.items()))
    body_len = len(response.request.body if response.request.body else [])
    request_size = method_len + headers_len + url_len + body_len    
    print(str(request_size) + " bytes sent for this file")
    #time.sleep(1) # optional to break up the number of requests
    return request_size
    
def open_file(full_path,total_lines,total_size):
    reader = open(full_path, mode='r')
    f = 0 # initiate line tracking
    request_size = 0
    for i, line in enumerate(reader, start=1):
        size = sys.getsizeof(line)
        request_size = request_size + size
        if line == '\n': # skip blank lines
            continue
        if (i % chunksize == 0 and i > 0): # before chunk size limit
            request_size = use_requests(chunk)
            del chunk[:]  # or: chunk = []
            total_size = size
            chunk.append(line)
        if (total_size > max_size): # before request size limit
            request_size = use_requests(chunk)
            del chunk[:]  # or: chunk = []
            total_size = size
            chunk.append(line)
        else:
            chunk.append(line)
        f = f+1
    reader.close
    print(str(total_lines) + " total lines processed for this file")
    return f,total_size

# Iterate over the directory specified
for root, sub, files in os.walk(directory):    
    for file in files:
        if (str(os.fsdecode(file)).startswith('.')): # Ignore hidden files
            continue
        else: 
            full_path = os.path.abspath(os.path.join(root, file))
            print(os.fsdecode(file)) # show the file being sent
            [f,t] = open_file(full_path,total_lines,total_size)
            total_lines = total_lines + f
            total_size = total_size + t
        total_files = total_files + 1

