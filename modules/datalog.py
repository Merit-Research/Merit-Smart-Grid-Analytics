#!/usr/bin/env python
# Filename:     datalog.py
# Authors:      apadin, yabskbd, mjmor, dvorva
# Start Date:   5/9/2016

"""
Driver for collecting data from ZWay server and saving it to given
location. This program also maintains a separate log file for providing
device information so that the data can be analyzed later

- Adrian Padin, 1/20/2017
"""

#==================== LIBRARIES ====================#
import os
import sys
import time
import datetime as dt
import csv
import io
import gzip
import shutil
import tarfile
from Queue import Empty as QueueEmpty
from multiprocessing import Process, Queue


#==================== CLASSES ====================#

class Datalog(object):
    """Wrapper for reading and writing sequential data to CSV files"""

    def __init__(self, folder, prefix, header):
        """Specify the prefix of the files you want to write.
        - prefix: The files will have the format "prefix_YYYY-MM-DD.csv"
        - header: List of names of each column (exclude "timestamp")

        Filesystem has the following format:

        folder (top-level)
            prefix
                month
                    day
        """

        # Initialize the child process
        child = DatalogChild(folder, prefix, header)
        self.queue = Queue()
        child_proc = Process(target=child.run, args=[self.queue])
        child_proc.start()
        
    def log(self, sample, timestamp=None):
        """Add a new sample to the log files.
        - sample: List of values to be written (excluding timestamp)
        """
        if (timestamp == None):
            timestamp = time.time()
        sample.insert(0, timestamp)
        self.queue.put(list(sample))
        
        
class DatalogChild(object):
        
    def __init__(self, folder, prefix, header):

        # Make sure the folder does not end with '/'
        self.folder = folder.rstrip('/')
        self.prefix = prefix
        self.date = None 
        self.fname = ""
        
        # Generate the header
        header.insert(0, "timestamp")
        output = io.BytesIO()
        csv.writer(output).writerow(header)
        self.header = output.getvalue()
        
    def run(self, queue):
        """Continually listen for incoming data."""
        while True:
            try:
                sample = queue.get(timeout=5)
            except QueueEmpty:
                pass
            else:
                self.log(sample)
                
    def log(self, sample):
        """Add data to log files."""
        timestamp = sample[0]
        date = dt.date.fromtimestamp(timestamp)
        #print("self:{} date:{}".format(self.date,date)) 
        # Check if new file is needed
        if self.date == None:
            print("Firt Time runing: new file") 
            # Start new day, new file
            self.date = date
            self.fname = self.get_filename(date)
            self.start_new_file(self.fname)
            
        elif date != self.date:
            print("Compress")
            # Compress old file or files
            self.compress(self.date)
            # FIXME: Removed archive function errors 
	    #if date.month != self.date.month:
            #    self.archive(self.get_month_folder(self.date))
            
            # Start new day, new file
            self.date = date
            self.fname = self.get_filename(date)
            print("New day: {}".format(self.fname))
            self.start_new_file(self.fname)
            
        # Record the sample
        with open(self.fname, 'ab') as fh:
            csv.writer(fh).writerow(sample)

    def get_file_name(self, date):
        fname = "{}_{}.csv".format(self.prefix, date)
        return '/'.join([fname])
    
    def get_filename(self, date):
        fname = "{}_{}.csv".format(self.prefix, date)
        return '/'.join([self.folder, fname])
        
    def get_month_folder(self, date):
        month_folder = "{}-{:0>2}".format(date.year, date.month)
        return '/'.join([self.folder, self.prefix, month_folder])
        
    def start_new_file(self, fname):
        """Start a new file with the given filename."""
        try:
            with open(fname, 'rb') as fh:
                assert(self.header == fh.readline())
        except AssertionError:
            raise RuntimeError("""existing file with the same name but different settings already exists. Please choose another name for this file""")
        except IOError:
            with open(fname, 'wb') as fh:
                fh.write(self.header)

    def compress(self, date):
        """Compress the given file to a copy with the same name plus the .gz extension."""
        # Create necessary subfolders
        fgzname = self.get_file_name(date) 
        gzname = '/'.join([self.get_month_folder(date), (fgzname + '.gz')])
        fname = self.get_filename(date)
        try:
            os.makedirs(os.path.dirname(gzname))
        except OSError:
            pass
        # Copy file into zip file and delete original
        try:
            with open(fname, 'rb') as fin, gzip.open(gzname, 'wb') as fout:
                shutil.copyfileobj(fin, fout)
        except IOError:
            print "Could not compress", fname, "because it does not exist."
        else:
            os.remove(fname)
            print "Successfully compressed", fname, "to", gzname

    def archive(self, month):
        """Unzip every file in the folder and then archive and compress the folder."""
	print("Archive running")
        if folder == '': return
        folder = prefix + '/' + folder
        for f in os.listdir(folder):
            if f.endswith('.gz'):
                filename = folder + '/' + f
                try:
                    with gzip.open(filename, 'rb') as fin, open(filename, 'wb') as fout:
                        shutil.copyfileobj(fin, fout)
                except IOError:
                    print filename, "not found"
                    pass
        with tarfile.open(folder + ".tar.gz", "w:gz") as tarball:
            tarball.add(folder)
        shutil.rmtree(folder)



