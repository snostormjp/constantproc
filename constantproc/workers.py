import sys
import multiprocessing
from multiprocessing.managers import BaseManager
import queue
import subprocess
import threading
import logging
from datetime import datetime as dt
import traceback
import time

import projenv

def factorize_naive(n):
    """ A naive factorization method. Take integer 'n', return list of
        factors.
    """
    if n < 2:
        return []
    factors = []
    p = 2

    while True:
        if n == 1:
            return factors

        r = n % p
        if r == 0:
            factors.append(p)
            n = n / p
        elif p * p >= n:
            factors.append(n)
            return factors
        elif p > 2:
            # Advance in steps of 2 over odd numbers
            p += 2
        else:
            # If p == 2, get to 3
            p += 1

def sleep(num,wait=2):
    """ worker function"""
    print('Worker:' + str(num)+' staarted with wait '+str(wait))
    st=dt.now()
    wo=subprocess.Popen(['sleep',str(wait)])
    wo.wait()
    return ("worker "+str(num)+" returned after "+str((dt.now()-st).seconds))
