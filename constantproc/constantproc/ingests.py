import csv
import os
import json
from glob import glob as ls

def make_nums(N):
    """ Create N large numbers to factorize.
    """
    nums = [999]
    for i in range(0,N):
        nums.append(nums[-1] + 2)
    return nums

def factor_ingest(shared_job_q):
    '''This is demo ingest for factorize example as seen on
    https://eli.thegreenplace.net/2012/01/16/python-parallelizing-cpu-bound-tasks-with-multiprocessing/
    '''

    N = 999
    nums = make_nums(N)

    # The numbers are split into chunks. Each chunk is pushed into the job
    # queue.
    chunksize = 43
    for i in range(0, len(nums), chunksize):
        shared_job_q.put(nums[i:i + chunksize])

def csv_ingest(shared_jq,type,loc):
    '''simple csv ingest'''
    jobdict={}
    fl=ls(loc+'/*.csv')
    for f in fl:
        fd=open(f,'r')
        csv.DictReader.reader(f)
        fd.close()
    shared_job_q.put(jobdict)
