import unittest
import os
import ast
import inspect

from wordfreq import Worker, RemoteWorker, Master, Config, ls
from hashlib import sha1
from collections import Counter
from os.path import isfile, basename

def unittest_verbosity():
    """Return the verbosity setting of the currently running TextTestRunner,
       or 0 if none is running.

    """
    frame = inspect.currentframe()
    while frame:
        self = frame.f_locals.get('self')
        if isinstance(self, unittest.TextTestRunner):
            return self.verbosity
        frame = frame.f_back
    return 0

def shared_setup():
    os.system("rm -f data/input/*.txt data/*/*.json data/*.json")
    if unittest_verbosity() > 2:
        Config.verbose=True

class TestWorker(unittest.TestCase): 
  
    def setUp(self):
        shared_setup()
  
    def test_injest(self):
        uri='http://www.constitution.org/usdeclar.txt'
        Worker().injest(uri) 
        expected_sha1 = '15684690e8132044f378b4d4af8a7331c8da17b1'
        with open(Config.input_path+'/'+basename(uri)) as f:
            raw_file_contents = f.read()        
        self.assertEqual(sha1(raw_file_contents).hexdigest(),expected_sha1,'Wrong sha1 Ulysses')

      
    def test_write_freq(self):
        counter = Counter(one=1, two=2, three=3)
        filepath = Config.output_path+"/write_freq_test.json"
        Worker().write_freq(filepath,counter)
        with open(filepath) as f:
            raw_file_contents = f.read()
        result = ast.literal_eval(raw_file_contents)
        expected = {"one": 1, "two":2, "three": 3}
        self.assertDictEqual(result, expected, 'counter written to disk did not have right key/values') 
    
    def test_word_freq(self):
        filepath = "data/test/pledge.txt"
        self.assertTrue(isfile(filepath),'data/test/pledge.txt should exist')
        counter = Master([Config.export_path]).read_freq(Worker().word_freq(filepath))
        self.assertEqual(counter['pledge'],1,'the pledge should contain the word pledge once')
        self.assertEqual(counter['allegiance'],1,'the pledge should contain the word allegiance once')
        self.assertEqual(counter['united'],1,'the pledge should contain the word United once')
        self.assertEqual(counter['United'],0,'word_freq should count the word United as lowercase')
        self.assertEqual(counter['god'],1,'the pledge should contain the word God once')
    
    def test_export(self):
        pass #covered by test_process_input
    
    def test_process_input(self):
        os.system("cp data/test/*.txt %s" % Config.input_path)
        Worker().process_input()
        self.assertEqual(len(ls(Config.export_path)), len(ls(Config.input_path)) )
        
class TestRemoteWorker(unittest.TestCase):
  
    def setUp(self):
        shared_setup()

    def test_fetch(self):
        pass
    
    def test_synch(self):
        pass
      
class TestMaster(unittest.TestCase):
  
    def setUp(self):
        shared_setup()

    def test_synch_all_workers(self):
        pass
    
    def test_read_freq(self):
        pass #covered by test_tally
    
    def test_update_total(self):
        pass #covered by test_tally
    
    def test_tally(self):
        os.system("cp testdata/tally/tally_?.json %s" % Config.import_path)
        master = Master(Config.export_path,"data/test_tally.json")
        master.tally()
        result = master.read_freq(master.total_filename)
        expected = {"one": 1, "two":2, "three": 3}
        self.assertDictEqual(result, expected, 'counter written to disk did not have right key/values') 
    
    def test_output(self):
        expected ='''the: 3
and: 2
of: 2
for: 2
to: 2
all: 1
pledge: 1
allegiance: 1
america: 1
one: 1
'''
        os.system("cp testdata/output/pledge.json %s" % Config.import_path)
        master = Master(Config.export_path,"data/test_output.json")
        master.tally()
        output = master.output()
        self.assertEqual(output, expected, 'Pledge output not as expected')
        
class TestEndToEnd(unittest.TestCase):
  
    expected_output1 ='''the: 56510
and: 37915
to: 27984
of: 27884
a: 22899
i: 22159
in: 17366
it: 15182
that: 14578
was: 13184
'''  

    expected_output2 ='''the: 56588
and: 37972
to: 28049
of: 27964
a: 22914
i: 22159
in: 17387
it: 15188
that: 14591
was: 13184
'''

    def setUp(self):
        shared_setup()

    def test_local_end_to_end(self):
        os.system("cp data/test/*.txt %s" % Config.input_path)
        worker = Worker()
        worker.process_input()
        master=Master([Config.export_path])
        master.synch_all_workers()
        master.tally()
        output1=master.output(10)
        self.assertEqual(output1, TestEndToEnd.expected_output1, 'End to End output1 not as expected')

        uri='http://www.constitution.org/usdeclar.txt'
        worker.injest(uri)
        worker.process_input()
        master.synch_all_workers()
        master.tally()
        output2=master.output(10)
        self.assertEqual(output2, TestEndToEnd.expected_output2, 'End to End output2 not as expected')


if __name__ == "__main__":
   unittest.main()
