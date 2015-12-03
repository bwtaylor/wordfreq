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

### Configuration

Setup ssh certificates so that files can be copied via scp from each worker's user@host to the master.

### Processing Flow

We use a file based processing flow. Since file writes are not atomic, we always follow a write
with a copy, so that downstream operations are never performed on partially written files. 

The following directories represent the various stages of processing:

```
data/input     - each worker looks here for text files to process
data/output    - each worker drops word frequency json files here
data/export    - eacher worker does an atomic copy of completed world frequency json files here
data/import    - the master fetcher does an atomic copy of staging files here
data/completed - the master adder does an atomic copy of processed word frequency files here
```

There are three processing components:

* Worker Word Frequency Generator (word_freq.py) - processes raw text into word frequency json files
* Master Fetcher (fetcher.py) - copies word frequency json files from workers to master via scp
* Master Adder (adder.py) - aggregates word frequencies into the overall total

Note that data acquisition, obtaining the raw text data and putting it in the appropriate input directory, is not specified by the problem. Some sample and test data is provided. Test and sample processing will copy data files as part of setup.
 