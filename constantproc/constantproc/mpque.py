import sys
import multiprocessing
from multiprocessing.managers import BaseManager
import queue
import subprocess
import logging
from datetime import datetime as dt
import traceback
import time

import projenv as penv
import ingests as ing
import workers as wkr





def sleep(job_q, result_q,logi):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q - each job a list of numbers to factorize. When the job is done,
        the result (dict mapping number -> list of factors) is placed into
        result_q. Runs until job_q is empty.
    """
    outdict={}
    try:
        job = job_q.get_nowait()
        logi.info(job)
        logi.info("type for job q object is "+str(type(job)))
        # eval job obj and do something return outdict to result q
        logi.info(outdict)
        result_q.put(outdict)
        return 0
    except queue.Empty:
        return -1


def factorizer_worker(job_q, result_q,logi):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q - each job a list of numbers to factorize. When the job is done,
        the result (dict mapping number -> list of factors) is placed into
        result_q. Runs until job_q is empty.
    """
    try:
        job = job_q.get_nowait()
        logi.info(job)
        outdict = {n: factorize_naive(n) for n in job}
        logi.info(outdict)
        result_q.put(outdict)
        return 0
    except queue.Empty:
        return -1


def mp_factorizer(shared_job_q, shared_result_q, nprocs,logi):
    """ Split the work with jobs in def runserver():
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, AUTHKEY)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    N = 999
    nums = make_nums(N)

    # The numbers are split into chunks. Each chunk is pushed into the job
    # queue.
    chunksize = 43
    for i in range(0, len(nums), chunksize):
        shared_job_q.put(nums[i:i + chunksize])

    # Wait until all results are ready in shared_result_q
    numresults = 0
    resultdict = {}
    while numresults < N:
        outdict = shared_result_q.get()
        resultdict.update(outdict)
        numresults += len(outdict)

    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(2)
    manager.shutdown()shared_job_q and results in
        shared_result_q into several processes. Launch each process with
        factorizer_worker as the worker function, and wait until all are
        finished.
    """
    procs = []
    delprocs = []
    sjq_sz=shared_job_q.qsize()
    if sjq_sz == None:
        sjq_sz = 0
    while sjq_sz > 0:
        delprocs = []
        if len(procs) < nprocs:
            p = multiprocessing.Process(
                    target=factorizer_worker,
                    args=(shared_job_q, shared_result_q,logi))
            procs.append(p)
            logi.info("Starting Process "+str(p))
            p.start()
        else:
            for p in procs:
                ec=p.exitcode
                if ec != None:
                    logi.info("Exit code was " + str(ec))
                    p.join()
                    delprocs.append(p)
        if len(delprocs) > 0:
            for p in delprocs:
                logi.info("Deleting process "+str(p))
                i2d=procs.index(p)
                procs.__delitem__(i2d)
        sjq_sz = shared_job_q.qsize()
        if sjq_sz == None:
            sjq_sz = 0


def mp_fproc(shared_job_q, shared_result_q, nprocs,logi):
    """ Reads processing dict from shared job q invokes async job
    {file:"",
    dirname:"",
    procdir:"",
    next_job="",False
    stop_job=""
    }
    """
    procs = []
    delprocs = []
    sjq_sz=shared_job_q.qsize()
    if sjq_sz == None:
        sjq_sz = 0
    while sjq_sz > 0:
        delprocs = []
        if len(procs) < nprocs:
            jobqo=shared_job_q.get_nowait()
            if 'nextproc' in jobqo:
                p = multiprocessing.Process(
                    target=jobqo['nextproc'],
                    args=(jobqo, shared_result_q,logi))
                procs.append(p)
                logi.info("Starting Process "+str(p))
                p.start()
        else:
            for p in procs:
                ec=p.exitcode
                if ec != None:
                    logi.info("Exit code was " + str(ec))
                    p.join()
                    delprocs.append(p)
        if len(delprocs) > 0:
            for p in delprocs:
                logi.info("Deleting process "+str(p))
                i2d=procs.index(p)
                procs.__delitem__(i2d)
        sjq_sz = shared_job_q.qsize()
        if sjq_sz == None:
            sjq_sz = 0


def make_server_manager(ip,port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = multiprocessing.Queue()
    result_q = multiprocessing.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class JobQueueManager(multiprocessing.managers.SyncManager):
        pass

    JobQueueManager.register('get_job_q', callable=lambda: job_q)
    JobQueueManager.register('get_result_q', callable=lambda: result_q)

    manager = JobQueueManager(address=(ip, port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def runserver(IP,PORTNUM,AUTHKEY,loglevel=logging.INFO):
    # Start a shared manager server and access its queues
    logger = multiprocessing.log_to_stderr()
    logger.setLevel(loglevel)
    manager = make_server_manager(IP, PORTNUM, AUTHKEY)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()
    downtime=10
    N = 0
    M = 0
    while True:
        try:
            N=shared_job_q.qsize()
            logger.info("Initial Job Que Objects "+str(N))
            # Wait until all results are ready in shared_result_q
            resultdict = {}
            while True:
                outdict={}
                M=shared_result_q.qsize()
                if N > 0:
                    if M > 0:
                        outdict = shared_result_q.get()
                        resultdict.update(outdict)
                        print(resultdict)
                        logger.info(outdict)
                    M=shared_result_q.qsize()
                    logger.info("MP Server Shared Job Q %s Vs. Shared Result Q %s" %(N,M))
                else:
                    time.sleep(downtime)
                    if downtime < 300:
                        downtime+=10
                N=shared_job_q.qsize()

        except Exception as e:
            logger.error(traceback.format_exc())
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(2)
    print("Shutting Down Server")
    manager.shutdown()


def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager


def runclient(IP,PORTNUM,AUTHKEY,loglevel=logging.INFO):
    logger = multiprocessing.log_to_stderr()
    logger.setLevel(loglevel)
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    downtime=10
    while True:
        try:
            mp_fproc(job_q, result_q, 4,logger)
            downtime=10
        except Exception as e:
            logger.error("exception was "+str(e))
            logger.error(traceback.format_exc())
            time.sleep(downtime)
            if downtime < 300:
                downtime+=10


def ingest_pick(shared_jq,type,loc):
    '''runs ingest type '''
    itypes=['csv','factor']
    if type not in itypes:
        return
    if type == 'csv':
        ing.csv_ingest(shared_jq,type,loc)
    elif type == 'factor':
        ing.factor_ingest(shared_jq)


def runclient_ingest(IP, PORTNUM, AUTHKEY, loglevel=logging.INFO, ingesttype='csv', ingest_loc=''):
    logger = multiprocessing.log_to_stderr()
    logger.setLevel(loglevel)
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    downtime=10
    N = 0
    M = 0
    ingest_pick(job_q, ingesttype, ingest_loc)
    while True:
        try:
            N=job_q.qsize()
            logger.info("Initial Job Que Objects "+str(N))
            # Wait until all results are ready in shared_result_q
            while True:
                M=result_q.qsize()
                if N > 1:
                    M=result_q.qsize()
                    logger.info("Client Ingest : Shared Job Q %s Vs. Shared Result Q %s" %(N,M))
                    time.sleep(downtime)
                    if downtime < 300:
                        downtime+=10
                else:
                    downtime=10
                    ingest_pick(job_q, ingesttype, ingest_loc)
                N=job_q.qsize()

        except Exception as e:
            logger.error(traceback.format_exc())


if __name__ == '__main__':

    if sys.argv[1] == 'start_server':
        runserver('192.168.2.28',50000,'abc'.encode('ASCII'))
    elif sys.argv[1] == 'start_client':
        runclient('192.168.2.28',50000,'abc'.encode('ASCII'))
    elif sys.argv[1] == 'start_ingest':
        runclient_ingest('192.168.2.28',50000,'abc'.encode('ASCII'),ingesttype='csv',ingest_loc='')

