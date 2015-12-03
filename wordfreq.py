#!/usr/local/bin/python

import re
import json
import io
import os
import sys
import getopt
import subprocess

from collections import Counter
from os import listdir, stat
from os.path import isfile, join, basename
from shutil import move
from hashlib import sha1

verbose = False
check_only = False
split_regex = re.compile('[a-z0-9]+')
ssh_path_regex = re.compile('(.*):(.*)$')
input_path = "data/input"
output_path = "data/output"
export_path = "data/export"
import_path = "data/import"
complete_path = "data/complete"
total_filename = "data/total.json"
workers = []
  
def write_freq(filepath, counter):
  json_file = io.open(filepath, 'w', encoding='utf8')
  data = json.dumps(counter, ensure_ascii=False, encoding='utf8')
  json_file.write(data)
  json_file.close()  

def word_freq(filepath):
  f = open(filepath)
  raw_file_contents = f.read().lower()
  f.close()
  json_filename = sha1(raw_file_contents).hexdigest() + ".json"
  output_filepath = join(output_path, json_filename)
  export_filepath = join(export_path, json_filename) 
  if isfile(export_filepath): 
    if verbose:
      print("Skipped word_freq of %s as %s already exists" % (filepath, export_filepath) )
    return ''
  else: 
    write_freq(output_filepath, Counter(split_regex.findall(raw_file_contents)))
    if verbose:
      print("Wrote word_freq of %s to %s" % (filepath, output_filepath) )
    return output_filepath
    
  
def export(output_filepath):
  if output_filepath == '':
    return
  json_filename = basename(output_filepath)
  export_filepath = join(export_path, json_filename)
  move(output_filepath, export_filepath)
  if verbose:
    print("Exported %s to %s" % (output_filepath, export_filepath))
    
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

def fetch(ssh_path, filename):
  remote_filename = ssh_path+"/"+filename
  completed_filename = complete_path+"/"+filename
  if not isfile(completed_filename):
    returncode = subprocess.call(["scp", remote_filename, import_path])
    if verbose and returncode==0:
      print("fetched %s to %s" % (remote_filename, import_path))
  else:
    if verbose:
      print("skip fetch of %s as %s already exist" % (remote_filename, completed_filename))
    

def synch(ssh_path):
  if verbose:
    print("synching %s" % ssh_path)
  export_path = extract_path(ssh_path)
  ssh_host = extract_host(ssh_path)
  if len(ssh_host) > 0:
    raw_output = subprocess.check_output(["ssh", ssh_host, "ls %s" % export_path])
  else:
    raw_output = subprocess.check_output(["ls", export_path])
  lines = raw_output.split('\n')
  for filename in lines[0:-1]:
    fetch(ssh_path,filename)
    
def read_freq(filename):
  try:
    f = open(filename)
    freq_raw = f.read()
    f.close()
    return Counter( clean_unicode(json.loads(freq_raw)) )
  except IOError:
    return Counter()
    
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
    
def process_input():    
  input_filepaths = [join(input_path, f) for f in listdir(input_path) if isfile(join(input_path, f)) and not f.startswith('.')]
  for input_filepath in input_filepaths:
    if verbose:
      print("processing %s" % input_filepath)
    export(word_freq(input_filepath))
  if verbose:
    print("done processing input")
        
def check(check_only):
  if (stat(output_path).st_dev != stat(export_path).st_dev):
    message = "Export path %s must be same partition as Output path %s for atomic copy"
    sys.exit(message % (export_path, import_path))
  if check_only:
    if verbose:
      print('check flag set, exiting after checking')
    sys.exit()
    
def verbose(is_on):
  global verbose
  verbose = is_on
  if verbose:
    print("verbose output on")

def help():
  print 'wordfreq.py mode [-h|--help] [-v|--verbose] [-c|--check] [ssh_path]*' 
  
def main(argv):
  check_only = False
  try:
    opts, args = getopt.getopt(argv,"hvc",['help','verbose','check'])
  except getopt.GetoptError:
    print("error")
    help()
    sys.exit(2)
  print('starting')
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      help()
      sys.exit()
    elif opt in ("-v", "--verbose"):
      verbose(True)
    elif opt in ("-c", "--check"):
      check_only = True
  if len(args)<1:
    help()
    sys.exit(2)
  mode = args[0]
  if mode == "worker":
    if verbose:
      print("worker mode")
    check(check_only)
    process_input()
  elif mode == "master":
    for worker in args[1:]:
      synch(worker)
      tally()
    output(10)

if __name__ == "__main__":
   main(sys.argv[1:])
