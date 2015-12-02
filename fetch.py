#!/usr/local/bin/python

import subprocess

staging_directory = "data/staging"

user_host1 = "btaylor@chess.clintlabs.us"
remote_data_path = "wordfreq/data"

def fetch(remote_filename):
  subprocess.call(["scp", remote_filename, staging_directory])

def synch(user_host):
  remote_cmd = "ls %s/test" % remote_data_path
  raw_output = subprocess.check_output(["ssh", user_host, remote_cmd])
  lines = raw_output.split('\n')
  for filename in lines[0:-1]:
    fetch(user_host+":"+remote_data_path+"/test/"+filename)
    
synch(user_host1)



