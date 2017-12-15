''' vaastr.py # proposed paper solution algorithm
implement VAAS Interchange algorithm for sliding and hopping windows
each function/class should take in a window and return the sample set
'''

from Sampler import AbstractHoppingSampler, DataSet, DataPoint
from threading import *
import numpy as np


# to get this impl to work
import random # for resevoir sampling
import sklearn

def loadClusters(choice_lengths):
    ''' assumes code is run in root of project '''
    import pickle
    available_clusters = {
     1: 'KMeans.cluster_count=1.pickle',
     11: 'KMeans.cluster_count=11.pickle',
     16: 'KMeans.cluster_count=16.pickle',
     21: 'KMeans.cluster_count=21.pickle',
     26: 'KMeans.cluster_count=26.pickle',
     31: 'KMeans.cluster_count=31.pickle',
     36: 'KMeans.cluster_count=36.pickle',
     41: 'KMeans.cluster_count=41.pickle',
     46: 'KMeans.cluster_count=46.pickle',
     51: 'KMeans.cluster_count=51.pickle',
     56: 'KMeans.cluster_count=56.pickle',
     6: 'KMeans.cluster_count=6.pickle'}

    clfs = dict()
    choice_lengths = set(choice_lengths)
    for cluster_count,filename in available_clusters.items():
        if cluster_count in choice_lengths:
            clfs[cluster_count] = pickle.load(open('/Users/William/EECS/vaastrLocal/cluster_data/' + filename, 'rb'))
    return {count: clf.cluster_centers_ for count,clf in clfs.items()}


class WillsSampler(AbstractHoppingSampler):
    ''' we'll see what works here and then write about it for 598 '''

    def __init__(self, sample_size, column_range, eta, parallel_count=1, cluster_centers=[]):
        AbstractHoppingSampler.__init__(self, sample_size, column_range, eta)
        if parallel_count < 1: raise ValueError("Cannot run split the window in fewer than 1 pieces")
        self.parallel_count = parallel_count
        if cluster_centers != []:
            self.set_cluster_center(cluster_centers)
            self._shrink = self._shrink_by_cluster
            self._insert_and_updateSet = self.insert_and_updateSet_cluster
        else:
            self.use_clusters = False
            self.insert_and_updateSet = self.insert_and_updateSet_naive
            self._shrink = self._shrink_naive

    def insert_and__update(self): return None # virtual name
    def _shrink(self): return None # virtual thingy

    def set_cluster_center(self, cluster_centers):
        self.cluster_centers = [DataPoint(center) for center in cluster_centers]
        self.use_clusters = True
        self.insert_and_updateSet = self.insert_and_updateSet_cluster

    def insert_and_updateSet_cluster(self, sample_set, point):
        rsp = 0
        for center in self.cluster_centers:
            rsp += self.proximity(point, center)
        sample_set.insert_new_point(point, rsp)
        return sample_set

    def _shrink_by_cluster(self, sample_set):
        """
        reduce sample set by one point using cluster comparison mode
        :param sample_set:
        :return: list of pairs
        """
        if not sample_set.isUsingPQmode():
            raise TypeError("sample_set must be in priority based mode")

        sample_set.remove()

        return sample_set

    def sample(self, window):
        """
        sample input window, return sampled dataset
        :param window: list of points
        :return: reduced list of points
        """
        sample_sets = []
        for i in range(self.parallel_count):
            sample_sets.append(DataSet([], usePQ=self.use_clusters))

        # split the window into multiple lengths
        divided_windows = [[] for _ in range(self.parallel_count)]

        try:
            while True:
                w = random.randint(0, self.parallel_count - 1)
                point = next(window)
                divided_windows[w].append(point)
        except StopIteration:
            # do nothing because nothing needs to be done in this loo[
            pass

        def sample_in_window(i):
            dwindow = divided_windows[i]
            for point in dwindow:
                sample_sets[i] = self._grow(sample_sets[i], np.array(point, dtype=np.float64))

        # Parallel(n_jobs=-1)(delayed(sample_in_window)(i) for i in range(len(divided_windows)))

        threads = []
        for i in range(self.parallel_count):
            threads.append(Thread(group=None,target=sample_in_window, args=([i])))
            threads[len(threads) - 1].start()

        # join on threads
        for thread in reversed(threads): thread.join()

        # flatten the sample set
        flattend_sample_set = DataSet([])
        for sample_set in sample_sets:
            flattend_sample_set += sample_set # operator overloaded to concatenate datasets
        return flattend_sample_set

    def _shrink_naive(self, sample_set):
        """
        reduce sample set by one point
        :param sample_set:
        :return: list of pairs
        """
        # get point with max rsp
        max_key_i = 0
        for i in range(1, len(sample_set)):
            if sample_set[i].get_rsp() > sample_set[max_key_i].get_rsp(): max_key_i = i

        max_key = sample_set[max_key_i].getPoint()
        # remove point with max rsp
        for point in range(len(sample_set)):
            sample_set[point].removefrom_rsp(self.proximity(sample_set[point].getPoint(), max_key))
        sample_set.remove(max_key_i)
        return sample_set

    def _grow(self, sample_set, point):
        """
        :param sample_set: DataSet
        :param point: ndarray
        :return: (updated) DataSet
        """
        self.insert_and_updateSet(sample_set, point)
        if len(sample_set) >= self.sample_size:
            if self.use_clusters:
                sample_set = self._shrink_by_cluster(sample_set)
            else:
                sample_set = self._shrink(sample_set)
        return sample_set

    def __str__(self):
        return "WillsSampler"

    def persistent_filename(self, filename,windowSize):
        num_clusters = len(self.cluster_centers) if self.use_clusters else 0
        return "{}.sampler=WillsSampler.w={}.sample_size={}.parallel_count={}.cluster_count={}.csv".format(filename, windowSize, self.sample_size, self.parallel_count, num_clusters)

    def __repr__(self): return self.__str__()