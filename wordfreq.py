#!/usr/local/bin/python

import re
import json
import io
import os
import sys
import getopt

from collections import Counter
from os import listdir, stat
from os.path import isfile, join, basename
from shutil import move

verbose = False
split_regex = re.compile('[a-z0-9]+')
input_path = "data/input"
output_path = "data/output"
export_path = "data/export"
  
def write_freq(filepath, counter):
  with io.open(filepath, 'w', encoding='utf8') as json_file:
    data = json.dumps(counter, ensure_ascii=False, encoding='utf8')
    json_file.write(unicode(data))    

def word_freq(filepath):
  raw_file_contents = open(filepath).read().lower()
  words = split_regex.findall(raw_file_contents)
  wordfreq = Counter(words)
  json_filename = basename(filepath) + ".json"
  output_filepath = join(output_path, json_filename)
  write_freq(output_filepath, wordfreq)
  if verbose:
    print("Wrote word_freq of %s to %s" % (filepath, output_filepath) )
  return output_filepath
  
def export(output_filepath):
  json_filename = basename(output_filepath)
  export_filepath = join(export_path, json_filename)
  move(output_filepath, export_filepath)
  if verbose:
    print("Exported %s to %s" % (output_filepath, export_filepath))
    
def process_input():    
  input_filepaths = [join(input_path, f) for f in listdir(input_path) if isfile(join(input_path, f)) and not f.startswith('.')]
  for input_filepath in input_filepaths:
    if verbose:
      print("processing %s" % input_filepath)
    export(word_freq(input_filepath))
  if verbose:
    print("done processing input")
    
def check():
  if verbose:
    print("verbose output on")
    print("input_path=%s" % input_path)
    print("output_path=%s" % output_path)
    print("export_path=%s" % export_path)
  if (stat(output_path).st_dev != stat(export_path).st_dev):
    message = "Export path %s must be same partition as Output path %s for atomic copy"
    sys.exit(message % (export_path, import_path))
  
def help():
  print 'wordfreq.py [-h|--help] [-v|--verbose] [ssh_path]*' 
  
def main(argv):
  global verbose
  try:
    opts, args = getopt.getopt(argv,"hv",['verbose','help'])
  except getopt.GetoptError:
    help()
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      help()
      sys.exit()
    elif opt in ("-v", "--verbose"):
      verbose = True
    else:
      print "opt: %s => %s" % (opt,arg)
  print('starting')
  check()
  process_input()

if __name__ == "__main__":
   main(sys.argv[1:])
