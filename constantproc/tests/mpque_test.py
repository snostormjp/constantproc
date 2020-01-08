import unittest
import threading
import logging


class MyTestCase(unittest.TestCase):


    def test_something(self):
        self.assertEqual(True, False)


    def serial_factorizer(nums):
        return {n: factorize_naive(n) for n in nums}

    def threaded_factorizer(nums, nthreads):
        def worker(nums, outdict):
            """ The worker function, invoked in a thread. 'nums' is a
                list of numbers to factor. The results are placed in
                outdict.
            """
            for n in nums:
                outdict[n] = factorize_naive(n)
        # Each thread will get 'chunksize' nums and its own output dict
        chunksize = int(math.ceil(len(nums) / float(nthreads)))
        threads = []
        outs = [{} for i in range(nthreads)]
        for i in range(nthreads):
            # Create each thread, passing it its chunk of numbers to factor
            # and output dict.
            t = threading.Thread(
                    target=worker,
                    args=(nums[chunksize * i:chunksize * (i + 1)],
                          outs[i]))
            threads.append(t)
            t.start()
        # Wait for all threads to finish
        for t in threads:
            t.join()
        # Merge all partial output dicts into a single dict and return it
        return {k: v for out_d in outs for k, v in out_d.iteritems()}


if __name__ == '__main__':
    unittest.main()
