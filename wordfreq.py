#!/usr/local/bin/python

import re
import json
import io
import os
import sys
import getopt
import subprocess
import unittest

from collections import Counter
from os import listdir, stat
from os.path import isfile, join, basename
from shutil import move
from hashlib import sha1
from urllib import urlretrieve

class Config(object):
    verbose = False
    injest_path = "data/injest"
    input_path = "data/input"
    output_path = "data/output"
    export_path = "data/export"
    import_path = "data/import"    
    complete_path = "data/complete"
    default_total_filename="data/total.json"

class Worker(object): 
          
    def __init__(self):
        if Config.verbose:
            print("worker started")
 
    def injest(self,uri):
        filename = basename(uri)
        injest_filename = Config.injest_path+"/"+filename
        input_filename = Config.input_path+"/"+filename
        urlretrieve(uri,injest_filename)
        move(injest_filename,input_filename)
        if Config.verbose:
            print("injested %s to %s" %(uri,input_filename))
  
  
    def write_freq(self,filepath,counter):
        with io.open(filepath, 'w', encoding='utf8') as json_file:
            data = json.dumps(counter, ensure_ascii=False, encoding='utf8')
            json_file.write(data) 
            

    def word_freq(self,filepath):
      
        with open(filepath) as f:
            raw_file_contents = f.read().lower()
        
        json_filename = sha1(raw_file_contents).hexdigest() + ".json"
        output_filepath = join(Config.output_path, json_filename)
        export_filepath = join(Config.export_path, json_filename)
        
        split_regex = re.compile('[a-z0-9]+')
        
        if isfile(export_filepath): 
            if Config.verbose:
                print( "Skipped word_freq of %s as %s already exists" % 
                       (filepath, export_filepath) )
            return ''
        else: 
            self.write_freq(output_filepath, Counter(split_regex.findall(raw_file_contents)))
            if Config.verbose:
                print("Wrote word_freq of %s to %s" % (filepath, output_filepath) )
            return output_filepath 
            
  
    def export(self,output_filepath):
        if output_filepath == '':
            return
        json_filename = basename(output_filepath)
        export_filepath = join(Config.export_path, json_filename)
        move(output_filepath, export_filepath)
        if Config.verbose:
            print("Exported %s to %s" % (output_filepath, export_filepath)) 

    def process_input(self,input_path="data/input"):
        
        for input_filepath in ls(input_path):
            if Config.verbose:
                print("processing %s" % input_filepath)
            self.export(self.word_freq(input_filepath))
        
        if Config.verbose:                                        
            print("done processing input")
    


class RemoteWorker(object):    
    
    def __init__(self, ssh_path):
      
        self.ssh_path = ssh_path
        
        ssh_path_regex = re.compile('(.*):(.*)$')        
        match = re.search(ssh_path_regex, ssh_path)
        
        if match:
            self.is_remote = True
            self.user_at_host = self.match.group(1)
            self.remote_export_path = self.match.group(2)
        else:
            self.is_remote = False
            self.user_at_host = ""
            self.remote_export_path = ssh_path


    def fetch(self,filename):
        remote_filename = self.ssh_path+"/"+filename
        completed_filename = Config.complete_path+"/"+filename
        if not isfile(completed_filename):
            returncode = subprocess.call(["scp", remote_filename, Config.import_path])
            if Config.verbose and returncode==0:
                print("fetched %s to %s" % (remote_filename, Config.import_path))
        else:                 
            if Config.verbose:
                print("skip fetch of %s as %s already exist" % 
                      (remote_filename, completed_filename) )
                
    def synch(self):
        if Config.verbose:
            print("synching %s" % self.ssh_path)
        if self.is_remote:
            cmd = "ls %s" % self.remote_export_path
            raw_output = subprocess.check_output(["ssh", self.user_at_host, cmd])
        else:
            raw_output = subprocess.check_output(["ls", self.remote_export_path])
        lines = raw_output.split('\n')
        for filename in lines[0:-1]:
            self.fetch(filename)
  

      
class Master(object):
    
    
    def __init__(self, ssh_paths, total_filename=Config.default_total_filename):
        self.remote_workers = [RemoteWorker(ssh_path) for ssh_path in ssh_paths]
        self.total_filename = total_filename
        self.local_worker = Worker()
            
    def synch_all_workers(self):
        for remote_worker in self.remote_workers:
            remote_worker.synch()
      
    def read_freq(self,filename):
        try:
            with open(filename) as f:
                freq_raw = f.read()
            return Counter( clean_unicode(json.loads(freq_raw)) )
        except IOError:
            return Counter()
    
    def update_total(self,freq_counter):
        total_counter = self.read_freq(self.total_filename)
        total_counter.update(freq_counter)
        self.local_worker.write_freq(self.total_filename, total_counter)
        if Config.verbose:
            print("  found %s occurances of %s words" % 
                  ( sum(freq_counter.values()), len(freq_counter)) )
            print("  total %s occurances of %s words" % 
                  (sum(total_counter.values()), len(total_counter)) )
      
    def tally(self):
        for freq_filename in ls(Config.import_path):
            if Config.verbose:
                print("merging words frequencies from %s" % freq_filename)
            self.update_total(self.read_freq(freq_filename))
            move(freq_filename, Config.complete_path)
    
    def output(self,n=10,output=""):
        total_freq = self.read_freq(self.total_filename)
        for (k,v) in total_freq.most_common(n):
            output += "%s: %s\n" % (k,v)
        if Config.verbose:
          print(output)
        return output
            
           
# Utility function to deal with python's json library converting strings to unicode 
def clean_unicode(input):
    if isinstance(input, dict):
        return { clean_unicode(key):clean_unicode(value) 
                 for key,value in input.iteritems() }
    elif isinstance(input, list):
        return [clean_unicode(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input
        
def ls(path):
  return [ join(path, f) for f in listdir(path) if isfile(join(path, f)) and not f.startswith('.') ]  
                                                             
        
def check(check_only):
    if (stat(Config.output_path).st_dev != stat(Config.export_path).st_dev):
        message = "Export path %s must be same partition as Output path %s for atomic copy"
        sys.exit(message % (export_path, import_path))
    if check_only:
        if Config.verbose:
            print('check flag set, exiting after checking')
        sys.exit()
        
def help(mode=""):
    if mode=="get":
        print 'wordfreq.py get [uri]*'
    if mode=="rget":
        print 'wordfreq.py rget worker_ssh_path uri [uri]*'
    else:
        print 'wordfreq.py mode [-h|--help] [-v|--verbose] [-c|--check] [ssh_path]*'
    
def main(argv): 
  
    check_only = False
    outfile = Config.default_total_filename
    
    try:
        opts, args = getopt.getopt(argv,"hvco:",['help','verbose','check','out='])
    except getopt.GetoptError:
        print("error")
        help()
        sys.exit(2) 
        
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            help()
            sys.exit()
        elif opt in ("-v", "--verbose"):
            Config.verbose=True
        elif opt in ("-c", "--check"):
            check_only = True
        elif opt in ("-o", "--out="):
            outfile = arg
            
    if len(args)<1:
        help()
        sys.exit(2)
    mode = args[0] 
    
    if mode == "worker":
        check(check_only)
        Worker().process_input()
    elif mode == "master":
        if len(args[1:]) > 0:
          ssh_paths = args[1:]
        else:
          ssh_paths = [Config.export_path]
        master = Master(ssh_paths,outfile)
        master.synch_all_workers()
        master.tally()
        master.output(10)
    elif mode == "get":
      for uri in args[1:]:
        Worker().injest(uri)
    elif mode == "rget":
      if len(args)<2:
        help(mode)
      worker = args[1]
      uris = args[2:]
      rget(worker,uris)
    elif mode == "test":
      suite = unittest.TestLoader().discover('.')
      os.system("rm -f data/test*.json")      
      unittest.TextTestRunner(verbosity={True:3,False:2}[Config.verbose]).run(suite)
    
if __name__ == "__main__":
   main(sys.argv[1:])
