#!/usr/local/bin/python

import subprocess
import io
import json
import re
from collections import Counter
from os import listdir
from os.path import isfile, join
from shutil import move
        
verbose = True        

import_path = "data/import"
complete_path = "data/complete"
total_filename = "data/total.json"

remote_host = "btaylor@chess.clintlabs.us:wordfreq/data/export"
local_host = "data/export"
ssh_path_regex = re.compile('(.*):(.*)$')

def clean_unicode(input):
  if isinstance(input, dict):
    return {clean_unicode(key):clean_unicode(value) for key,value in input.iteritems()}
  elif isinstance(input, list):
    return [clean_unicode(element) for element in input]
  elif isinstance(input, unicode):
    return input.encode('utf-8')
  else:
    return input

def extract_path(ssh_path):
  match = re.search(ssh_path_regex, ssh_path)
  if match:
    return match.group(2)
  else:
    return ssh_path

def extract_host(ssh_path):
  match = re.search(ssh_path_regex, ssh_path)
  if match:
    return match.group(1)
  else:
    return ''
    

def fetch(remote_filename):
  returncode = subprocess.call(["scp", remote_filename, import_path])
  if verbose and returncode==0:
    print("fetched %s to %s" % (remote_filename, import_path)) 

def synch(ssh_path):
  export_path = extract_path(ssh_path)
  ssh_host = extract_host(ssh_path)
  if len(ssh_host) > 0:
    raw_output = subprocess.check_output(["ssh", ssh_path, "ls %s" % export_path])
  else:
    raw_output = subprocess.check_output(["ls", export_path])
  lines = raw_output.split('\n')
  for filename in lines[0:-1]:
    fetch(ssh_path+"/"+filename)
    
def read_freq(filename):
  try:
    f = open(filename)
    freq_raw = f.read()
    f.close()
    return Counter( clean_unicode(json.loads(freq_raw)) )
  except IOError:
    return Counter()
  
def write_freq(filepath, counter):
  json_file = io.open(filepath, 'w', encoding='utf8')
  data = json.dumps(counter, ensure_ascii=False, encoding='utf8')
  json_file.write(unicode(data)) 
  json_file.close()
  
def total(freq_counter):
  print("  found %s occurances of %s words" % (sum(freq_counter.values()), len(freq_counter)) )
  total_counter = read_freq(total_filename)
  total_counter.update(freq_counter)
  write_freq(total_filename, total_counter)
  print("  total %s occurances of %s words" % (sum(total_counter.values()), len(total_counter)) )
  
def tally():
  freq_filenames = [join(import_path, f) for f in listdir(import_path) if isfile(join(import_path, f)) and not f.startswith('.')]
  for freq_filename in freq_filenames:
    print("merging words frequencies from %s" % freq_filename)
    total(read_freq(freq_filename))
    move(freq_filename, complete_path)
    
def output(n):
  total_freq = read_freq(total_filename)
  for (k,v) in total_freq.most_common(n):
    print ( "%s: %s" % (k,v) )
  
synch(local_host)
tally()
output(10)


