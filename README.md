# wordfreq

Word frequency exercise

### Prerequisites and Installation

We assume these things are working on all nodes (master and worker) when we start:

* Python 2.7 is installed
* git is installed
* scp is installed and on the path

Install the code by using git:

```
git clone https://github.com/bwtaylor/wordfreq.git
```
No libraries are required.

### Configuration

Setup ssh certificates so that files can be copied via scp from each worker's user@host to the master.

For each such worker, we will use its ssh_host value with path. Any number positive number of workers is supported. Workers may also be local by simply supplying a file path. For convenience, set these to an environment shell variable on the master node.

The end to end test to integrate remote nodes synch will fail unless at least one such node is configured. This is done by the environment variable TEST_REMOTE_WORKERS, which must be set.

```
W1=user@host:/path/to/wordfreq
W2=user2@host2:relpath/wordfreq
W3=.
W4=other/local/worfreq/repo

export TEST_REMOTE_WORKERS="$W1 $W2"
```

### Processing Flow

We use a file based processing flow. Since file writes are not atomic, we sometimes must write to an intermediate directory and copy completed files to the directory where they become visible to other processes. This pattern prevents downstream operations are never performed on partially written files. 

The following directories represent the various stages of processing:

```
data/injest    - workers download files to here and move fully written files to data/input
data/input     - each worker looks here for text files to process
data/output    - each worker creates frequency json files here
data/export    - workers do an atomic copy of completed world frequency json files here
data/import    - the master fetcher does an atomic copy of staging files here
data/completed - the master adder does an atomic copy of processed word frequency files here
data           - the final tally file is typically written here, typically as data/total.json
```

### Code Components

There are five processing components, all in the wordfreq.py file. The first four are classes contained within the overall script.

* Worker - processes raw text into word frequency json files
* RemoteWorker - copies word frequency json files from workers to master via scp
* Master Adder - aggregates word frequencies into the overall total
* Config - struct for holding configuration variables
* top level script - handles CLI and other convenience functions

### Tutorial

Make sure you've set up ssh certificates and the ssh_paths in environment variables as shown in the configuration setion above.

Start by running the tests, with minor verbosity. Verbosity ranges 0 to 4 and defaults to 1. The -v flag sets it to 2, while -Vn (capital)sets it to n.To set it to other values, use -V3 or -V4 or -V0 instead of -v.

```
cd src/wordfreq
python wordfreq.py -v test
```
If you get an error in test_remote_workers, you didn't set your configuration properly. This will usually show as a KeyError: 'TEST_REMOTE_WORKERS'.

The worker is responsible for injesting data. Let's copy the pledge of allegiance into it's input folder and invoke the worker locally. Some sample data is provided in data/test

```
less data/test/pledge.txt
cp data/test/pledge.txt data/input
python wordfreq.py worker
```
It completes, but doesn't tell us much. 

```
python wordfreq.py clean
cp data/test/pledge.txt data/input
python wordfreq.py -V4 worker
```
This should show some output:

```
worker started
processing data/input/pledge.txt
Wrote word_freq of data/input/pledge.txt to data/output/cfaf9930079651c7c2d8d113b67a144ee8727761.json
Exported data/output/cfaf9930079651c7c2d8d113b67a144ee8727761.json to data/export/cfaf9930079651c7c2d8d113b67a144ee8727761.json
done processing input
```
Some things to note:
* Word frequency files are written to data/output
* Output is a json file
* The name of the file is a sha1 (of the lowercase input text)
* Once they are written, files are copied to data/export and where they can be fetched

Let's look at the output ```cat data/export/cfaf9930079651c7c2d8d113b67a144ee8727761.json```

```
{"and": 2, "all": 1, "pledge": 1, "allegiance": 1, "america": 1, "one": 1, "states": 1, "united": 1, "stands": 1, "for": 2, "justice": 1, "god": 1, "liberty": 1, "to": 2, "republic": 1, "which": 1, "under": 1, "it": 1, "flag": 1, "nation": 1, "with": 1, "indivisible": 1, "i": 1, "of": 2, "the": 3}
```
It's a JSON map with words as keys and positive integer counts. It's OK to copy data right into /data/input only if you know that the worker is off. It has a mode where it polls continually to look for input and when using this, there is a real danger that it will find a half written file, so the preferred way to injest data is with the get command (and rget for remote invokation).

```
python wordfreq.py clean
python wordfreq.py get data/test/pledge.txt
python wordfreq.py -V4 worker
```
Same result. SAFE! 

Try the last command again: ```python wordfreq.py -V4 worker```

```
worker started
processing data/input/pledge.txt
Skipped word_freq of data/input/pledge.txt as data/export/cfaf9930079651c7c2d8d113b67a144ee8727761.json already exists
done processing input
```
Notice that it skipped the input file. It's gloriously lazy about redoing work it knows it's already done.

You don't have to injest local files. They can be URIs: ```python wordfreq.py -V4 get http://www.constitution.org/usdeclar.txt``` will output:

```
worker started
injested http://www.constitution.org/usdeclar.txt to data/input/usdeclar.txt
```
Running the worker on max verbosity will report

```
worker started
processing data/input/pledge.txt
Skipped word_freq of data/input/pledge.txt as data/export/cfaf9930079651c7c2d8d113b67a144ee8727761.json already exists
processing data/input/usdeclar.txt
Wrote word_freq of data/input/usdeclar.txt to data/output/b21c7a6e4a343caad76b8d440229ffe340b65802.json
Exported data/output/b21c7a6e4a343caad76b8d440229ffe340b65802.json to data/export/b21c7a6e4a343caad76b8d440229ffe340b65802.json
done processing input
```
Now there are two files in the export folder. They will pile up forever unless we clean them, but it's probably OK to let them pile up. If you really needed the disk space, you could delete their contents, because it's just the file name that's preventing reprocessing.

Now we can run the master and have it fetch these two word frequency files, add them up and show the top 10 words in the output. We're still running everything locally, though.

We run the master with max verbosity with the command:

```
python wordfreq.py -V4 master
```
and it reports back:

```
worker started
synching .
Executing: cp -- ./data/export/b21c7a6e4a343caad76b8d440229ffe340b65802.json data/import
fetched ./data/export/b21c7a6e4a343caad76b8d440229ffe340b65802.json to data/import
Executing: cp -- ./data/export/cfaf9930079651c7c2d8d113b67a144ee8727761.json data/import
fetched ./data/export/cfaf9930079651c7c2d8d113b67a144ee8727761.json to data/import
merging words frequencies from data/import/b21c7a6e4a343caad76b8d440229ffe340b65802.json
  found 1341 occurances of 541 words
  total 1341 occurances of 541 words
merging words frequencies from data/import/cfaf9930079651c7c2d8d113b67a144ee8727761.json
  found 31 occurances of 25 words
  total 1372 occurances of 546 words
of: 82
the: 81
to: 67
and: 59
for: 31
our: 26
in: 21
their: 20
has: 20
he: 19
```
If we forget, we can ask it again. ```python wordfreq.py master``` will give just the results again.

The worker runs locally unless we tell it to run somewhere else. To do that, I can just login to another box and start it. Nothing's really different until I want the master to talk to it. Then I have to tell the master where it is. Let's throw everything at a remote worker. Shell in and do:

```
cd wordfreq
python wordfreq.py clean
cp data/test/*.txt data/input
python wordfreq.py -V3 worker

```
Now you'll see less verbose output:

```
worker started
Wrote word_freq of data/input/tom_sawyer.txt to data/output/5813c2959155df3aea5b39a61d3b60e4edc7001d.json
Exported data/output/5813c2959155df3aea5b39a61d3b60e4edc7001d.json to data/export/5813c2959155df3aea5b39a61d3b60e4edc7001d.json
Wrote word_freq of data/input/moby_dick.txt to data/output/2bdd11f1adf0eb980a9a1f53e80de0f34e7c8c17.json
Exported data/output/2bdd11f1adf0eb980a9a1f53e80de0f34e7c8c17.json to data/export/2bdd11f1adf0eb980a9a1f53e80de0f34e7c8c17.json
Wrote word_freq of data/input/frankenstein.txt to data/output/25342d17955ad31d38799dfc296eb20df19d7d4b.json
Exported data/output/25342d17955ad31d38799dfc296eb20df19d7d4b.json to data/export/25342d17955ad31d38799dfc296eb20df19d7d4b.json
Wrote word_freq of data/input/tale_two_cities.txt to data/output/b9520aafda30b06e5273455ee2b7b2e65edac361.json
Exported data/output/b9520aafda30b06e5273455ee2b7b2e65edac361.json to data/export/b9520aafda30b06e5273455ee2b7b2e65edac361.json
Wrote word_freq of data/input/huck_finn.txt to data/output/ee3bfbe832840fe853b49a02c20c2a7e795834c9.json
Exported data/output/ee3bfbe832840fe853b49a02c20c2a7e795834c9.json to data/export/ee3bfbe832840fe853b49a02c20c2a7e795834c9.json
Wrote word_freq of data/input/dracula.txt to data/output/84da50d2770fbbac0ebf3825febb74f488217cea.json
Exported data/output/84da50d2770fbbac0ebf3825febb74f488217cea.json to data/export/84da50d2770fbbac0ebf3825febb74f488217cea.json
Wrote word_freq of data/input/pride_and_prejudice.txt to data/output/b2573cbe40d322bf40f147f28333ec001bc5c6a1.json
Exported data/output/b2573cbe40d322bf40f147f28333ec001bc5c6a1.json to data/export/b2573cbe40d322bf40f147f28333ec001bc5c6a1.json
Wrote word_freq of data/input/sherlock_holmes.txt to data/output/3a1bee8877c1ed268abf05ab3b008256fa1e4a8e.json
Exported data/output/3a1bee8877c1ed268abf05ab3b008256fa1e4a8e.json to data/export/3a1bee8877c1ed268abf05ab3b008256fa1e4a8e.json
Wrote word_freq of data/input/alice_wonderland.txt to data/output/c3dc0148cd04c1f69e837ef1f6f2cfb3efd9033a.json
Exported data/output/c3dc0148cd04c1f69e837ef1f6f2cfb3efd9033a.json to data/export/c3dc0148cd04c1f69e837ef1f6f2cfb3efd9033a.json
Wrote word_freq of data/input/pledge.txt to data/output/cfaf9930079651c7c2d8d113b67a144ee8727761.json
Exported data/output/cfaf9930079651c7c2d8d113b67a144ee8727761.json to data/export/cfaf9930079651c7c2d8d113b67a144ee8727761.json
```
Suppose that I followed the configuration section above and set up $W1 as a remote ssh_path. Maybe something like ```W1=bryan@mybox.mydomain.com:src/wordfreq```. Then I can invoke the master to talk to the worker like this:

```
python wordfreq.py -V3 master $W1
```
and it will show my files moving across he wire and being tallied:

```
worker started
synching btaylor@chess.clintlabs.us:wordfreq
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/25342d17955ad31d38799dfc296eb20df19d7d4b.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/2bdd11f1adf0eb980a9a1f53e80de0f34e7c8c17.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/3a1bee8877c1ed268abf05ab3b008256fa1e4a8e.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/5813c2959155df3aea5b39a61d3b60e4edc7001d.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/84da50d2770fbbac0ebf3825febb74f488217cea.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/b2573cbe40d322bf40f147f28333ec001bc5c6a1.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/b9520aafda30b06e5273455ee2b7b2e65edac361.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/c3dc0148cd04c1f69e837ef1f6f2cfb3efd9033a.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/cfaf9930079651c7c2d8d113b67a144ee8727761.json to data/import
fetched btaylor@chess.clintlabs.us:wordfreq/data/export/ee3bfbe832840fe853b49a02c20c2a7e795834c9.json to data/import
the: 56591
and: 37974
to: 28051
of: 27966
a: 22914
i: 22160
in: 17387
it: 15189
that: 14591
was: 13184
```
If we have multiple workers, we can just add more:

```
python wordfreq.py master $W1 $W2 $W3
```
We can also ask for data injestion remotely using rget instead of get:

```
python wordfreq.py rget $W1 http://www.constitution.org/usdeclar.txt
```
Finally, we can run the worker in a loop with worker-loop and stop it with worker-stop. We can run the master in a loop with master-loop and master-stop.

```
# on the worker node
python wordfreq.py clean
python wordfreq.py worker-loop
```
```
# on the master node
python wordfreq.py clean
python wordfreq.py -V3 master-loop $W1
```
While these run, I should be able to safely injest files that will be processed in near real time.

```
# on the MASTER node (or anywhere that can connect)
python wordfreq.py rget $W1 data/test/alice_wonderland.txt
python wordfreq.py rget $W1 data/test/sherlock_holmes.txt
python wordfreq.py rget $W1 http://www.constitution.org/usdeclar.txt

```
Finally, we stop the loops via ```python wordfreq.py master-stop``` on the master (in a separate shell) and ```python wordfreq.py worker-stop```.

