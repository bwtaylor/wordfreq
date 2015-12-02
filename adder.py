#!/usr/local/bin/python

import json
from collections import Counter
from os import listdir
from os.path import isfile, join

def clean_unicode(input):
    if isinstance(input, dict):
        return {clean_unicode(key):clean_unicode(value) for key,value in input.iteritems()}
    elif isinstance(input, list):
        return [clean_unicode(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

import_path = "data/import"

total_counter = Counter()
freq_files = [join(import_path, f) for f in listdir(import_path) if isfile(join(import_path, f))]
for freq_file in freq_files:
  freq_raw = open(freq_file).read()
  freq_counter = Counter( clean_unicode(json.loads(freq_raw)) )
  total_counter.update(freq_counter)
  print("merged %s word frequencies from %s" % (len(freq_counter), freq_file))
  
print( total_counter.most_common(40) )
  
