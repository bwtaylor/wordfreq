#!/usr/local/bin/python

import re
import json
import io
import os
import sys
import getopt
import subprocess
import time
import unittest

from collections import Counter
from os import listdir, stat
from os.path import isfile, join, basename, abspath
from shutil import move
from hashlib import sha1
from urllib import urlretrieve, pathname2url
from urlparse import urlparse

class Config(object):
    '''Holds config globals. There's probably a more pythonic way to do this.
    '''
    verbose = 1
    injest_path = "data/injest"
    input_path = "data/input"
    output_path = "data/output"
    export_path = "data/export"
    import_path = "data/import"    
    complete_path = "data/complete"
    default_total_filename="data/total.json"
    pidfile = "worker.pid"

class Worker(object):

    '''The Worker handles turning raw text files into word frequency JSON files.
    This includes safe concurrent file access during injestion and export. Only
    one worker process should run over a given direcory structure.
    '''
          
    def __init__(self):
        if Config.verbose > 2:
            print("worker started")
 
    def injest(self,uri):
        """Fetch file from uri (or uri converted from file path) and once
        fetch is complete, copy it. The client must assure the file at the
        URI is fully written prior to call. It is safe to run this concurrently
        with a worker running process_input(), as only fully formed files are
        moved to the input_path. This allows a Remote Worker may invoke this
        via ssh at the same time that another Worker is running.
        """
        filename = basename(uri)
        injest_filename = Config.injest_path+"/"+filename
        input_filename = Config.input_path+"/"+filename
        if Config.verbose > 3:
            print("injested %s to %s" %(uri,input_filename))
        parsed_uri = urlparse(uri)
        if parsed_uri.netloc:
            urlretrieve(uri,injest_filename)
        else:
            #convert possibly relative filepath to file: scheme uri
            file_uri = 'file://' + pathname2url(abspath(parsed_uri.path))
            urlretrieve(uri,injest_filename)
        move(injest_filename,input_filename)
  
  
    def write_freq(self,filepath,counter):
        """Spool out a counter object to a JSON file with specified filepath.
        """
        with io.open(filepath, 'w', encoding='utf8') as json_file:
            data = json.dumps(counter, ensure_ascii=False, encoding='utf8')
            json_file.write(data) 
            

    def word_freq(self,filepath):
        """Read the raw text input at filepath, convert to lowercaes, check
        if we've processed it before (using sha1 of lowercase content), and if
        not, split into words, and use a collections.Counter to tally word
        frequencies and write the output to disk. Return the output filepath
        if it was written, or empty string if it already existed.
        """
      
        with open(filepath) as f:
            raw_file_contents = f.read().lower()
        
        json_filename = sha1(raw_file_contents).hexdigest() + ".json"
        output_filepath = join(Config.output_path, json_filename)
        export_filepath = join(Config.export_path, json_filename)
        
        split_regex = re.compile('[a-z0-9]+')
        
        if isfile(export_filepath): 
            if Config.verbose > 3:
                print( "Skipped word_freq of %s as %s already exists" % 
                       (filepath, export_filepath) )
            return ''
        else: 
            self.write_freq(output_filepath, Counter(split_regex.findall(raw_file_contents)))
            if Config.verbose > 3:
                print("Wrote word_freq of %s to %s" % (filepath, output_filepath) )
            return output_filepath 
            
  
    def export(self,output_filepath):
        """Calls shutil.move on output file to make it visible in the export
        folder, so that RemoteWorkers fetching from the export folder 
        will never see partially written files.
        """
        if output_filepath == '':
            return
        json_filename = basename(output_filepath)
        export_filepath = join(Config.export_path, json_filename)
        move(output_filepath, export_filepath)
        if Config.verbose > 2:
            print("Exported %s to %s" % (output_filepath, export_filepath)) 

    def process_input(self,input_path="data/input"):
        """Loop over each file on the input path and export its word
        frequency, so that it becomes visible to RemoteWorkers in an atomic
        way.
        """
        
        for input_filepath in ls(input_path):
            if Config.verbose > 3:
                print("processing %s" % input_filepath)
            self.export(self.word_freq(input_filepath))
        
        if Config.verbose > 3:                                        
            print("done processing input")
    


class RemoteWorker(object): 
  
    """The RemoteWorker handles remote file synchning by the master to a
    given worker. Integration occurs vis ssh/scp if the Worker location is
    truly remote and via local operations if not.
    """
    
    def __init__(self, ssh_path):
      
        self.ssh_path = ssh_path
        
        ssh_path_regex = re.compile('(.*):(.*)$')        
        match = re.search(ssh_path_regex, ssh_path)
        
        if match:
            self.is_remote = True
            self.user_at_host = match.group(1)
            self.remote_path = match.group(2)
        else:
            # must be a local path
            self.is_remote = False
            self.user_at_host = ""
            self.remote_path = ssh_path


    def fetch(self,filename):
        """Use scp to physically move a file from the remote worker to the 
        import path. Will exclude files that have been processed. It's assumed
        the master only activates one Remote Worker at a time, so we don't worry
        about concurrent fetchings of the same file. The Master is responsible
        for assuring
        """
        
        remote_filename = self.ssh_path+"/"+Config.export_path+"/"+filename
        completed_filename = Config.complete_path+"/"+filename

        if Config.verbose > 3:                                                             
          verbose_flag = "-v"
        else:
          verbose_flag = "-q"
          
        if not isfile(completed_filename):
            returncode = subprocess.call(["scp", verbose_flag, remote_filename, Config.import_path])
            if Config.verbose > 2 and returncode==0:
                print("fetched %s to %s" % (remote_filename, Config.import_path))
        else:                 
            if Config.verbose > 2:
                print("skip fetch of %s as %s already exist" % 
                      (remote_filename, completed_filename) )

    def synch(self):
        """Uses ssh to list the files in the remote worker's export directory,
        and then fetches them.
        """
        
        remote_export_path = self.remote_path+"/"+Config.export_path
        
        if Config.verbose > 2:
            print("synching %s" % self.ssh_path)
            
        if self.is_remote:
            cmd = "ls %s" % remote_export_path
            raw_output = subprocess.check_output(["ssh", self.user_at_host, cmd])

        else:
            raw_output = subprocess.check_output(["ls", remote_export_path])

        filenames = raw_output.split('\n')[0:-1] #drop the last empty line
        for filename in filenames:
            self.fetch(basename(filename))

    def remote_injest(self,uris):
        """Remotely invokes the injest command of a worker."""

        uris_str = " ".join(uris)
        verbosity = "-V%s" % Config.verbose

        cmd = "cd %s; python wordfreq.py %s get %s" % (self.remote_path, verbosity, uris_str)

        if self.is_remote:
            raw_output = subprocess.check_output(["ssh", self.user_at_host, cmd])
        else:
            raw_output = subprocess.check_output(["python","wordfreq.py", verbosity, "get",uris_str])

        if Config.verbose > 2:
          print("invoked remote injest on worker at %s for %s" %
                (self.ssh_path, uris_str) )
          print("remote invocation output: %s" % raw_output )
          
    def process_input(self):
        '''Remotely invokes the worker to process its input'''
        
        verbosity = "-V%s" % Config.verbose

        cmd = "cd %s; python wordfreq.py %s worker" % (self.remote_path, verbosity)
        if self.is_remote:
            raw_output = subprocess.check_output(["ssh", self.user_at_host, cmd])
        else:
            raw_output = subprocess.check_output([cmd])
        
        if Config.verbose > 2:
           print( raw_output ) 
          
    def clean(self):
        '''cleans all the files out of the directory structure processing
        pipeline.
        '''

        if self.is_remote:
            cmd = "cd %s; python wordfreq.py clean" % (self.remote_path)
            subprocess.call(["ssh", self.user_at_host, cmd])
        else:
            path = self.remote_path;
            os.system("rm -f %s/data/test*.json %s/data/*/*.json %s/data/input/*.txt" % (path,path,path) )
      
class Master(object):
  
    """The Master is responsible for coordinating RemoteWorkers to synch to
    each worker's word frequency files, tally them, update the running total,
    and produce output.
    """
    
    
    def __init__(self, ssh_paths, total_filename=Config.default_total_filename):
        self.remote_workers = [RemoteWorker(ssh_path) for ssh_path in ssh_paths]
        self.total_filename = total_filename
        self.local_worker = Worker()
            
    def synch_all_workers(self):
        """Loop over every Remote Worker in turn and ask it to synch.
        """
        for remote_worker in self.remote_workers:
            remote_worker.synch()
      
    def read_freq(self,filename):
        """Read a JSON file into a counter.
        """
        try:
            with open(filename) as f:
                freq_raw = f.read()
            return Counter( clean_unicode(json.loads(freq_raw)) )
        except IOError:
            return Counter()
    
    def update_total(self,freq_counter):
        """Takes in a counter of word frequencies, reads a counter
        representing the running total from the appropriate file, adds
        the former to the latter, and writes it out again.
        """
        total_counter = self.read_freq(self.total_filename)
        total_counter.update(freq_counter)
        self.local_worker.write_freq(self.total_filename, total_counter)
        if Config.verbose > 3:
            print("  found %s occurances of %s words" % 
                  ( sum(freq_counter.values()), len(freq_counter)) )
            print("  total %s occurances of %s words" % 
                  (sum(total_counter.values()), len(total_counter)) )
      
    def tally(self):
        """Read each word frequency files in the import path and
        add it to the running total, then move the file to the directory
        where all the completed files go.
        """
        for freq_filename in ls(Config.import_path):
            if Config.verbose > 3:
                print("merging words frequencies from %s" % freq_filename)
            self.update_total(self.read_freq(freq_filename))
            move(freq_filename, Config.complete_path)
    
    def output(self,n=10,output=""):
        """Read the totals from the file that tracks them and output the
        top n most frequent words.
        """
        total_freq = self.read_freq(self.total_filename)
        for (k,v) in total_freq.most_common(n):
            output += "%s: %s\n" % (k,v)
        if Config.verbose > 2:
          print(output)
        return output
            
def clean_unicode(input):
    """Utility function to deal with python's json library converting
    strings to the unicode data type, which obfuscates things since we
    usually deal with ascii or utf-8 strings directl.
    """
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
    """Utility function to simply list the (non-hidden) contents of a directory.
    Returns a list of relative paths to each file of the dirctory."""
    return [ join(path, f) for f in listdir(path) if isfile(join(path, f)) and not f.startswith('.') ]
    
def check(check_only):
    if (stat(Config.output_path).st_dev != stat(Config.export_path).st_dev):
        message = "Export path %s must be same partition as Output path %s for atomic copy"
        sys.exit(message % (export_path, import_path))
    if check_only:
        if Config.verbose > 3:
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
        opts, args = getopt.getopt(argv,"hvV:co:",['help','verbose','verbosity=','check','out='])
    except getopt.GetoptError:
        print("error")
        help()
        sys.exit(2) 
        
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            help()
            sys.exit()
        elif opt in ("-v", "--verbose"):
            Config.verbose=2
        elif opt in ("-V", "--verbosity="):
            Config.verbose=int(arg)
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
        
    elif mode == "worker-loop":
        pid = str(os.getpid())
        if os.path.isfile(Config.pidfile):
            sys.exit("pidfile exists: worker already running")
        else:
            file(Config.pidfile, 'w').write(pid)
        worker = Worker()
        
        go = True
        while go:
            worker.process_input()
            time.sleep(1)
            try:
                with open(Config.pidfile) as f:
                    newpid = f.read()
                    go = (newpid == pid)
            except IOError:
                go = False
            
    elif mode == "worker-stop":
        os.system("rm -f %s" % Config.pidfile)
        
    elif mode == "master":
        if len(args[1:]) > 0:
            ssh_paths = args[1:]
        else:
            ssh_paths = ['.']
        master = Master(ssh_paths,outfile)
        master.synch_all_workers()
        master.tally()
        master.output(10)
        
    elif mode == "get":
        worker = Worker()
        for uri in args[1:]:
            worker.injest(uri)
            
    elif mode == "rget":
        if len(args)<2:
            help(mode)
        remote_worker = RemoteWorker(args[1])
        uris = args[2:]
        remote_worker.remote_injest(uris)
    elif mode == "clean":
        if len(args)<2:
          ssh_path="."
        else:
          ssh_path=args[1]
        remote_worker = RemoteWorker(ssh_path)
        remote_worker.clean()
        
    elif mode == "test":
        suite = unittest.TestLoader().discover('.')
        os.system("rm -f data/test*.json")
        unittest.TextTestRunner(verbosity=int(Config.verbose)).run(suite)
    
if  __name__ == "__main__":
    main(sys.argv[1:])
