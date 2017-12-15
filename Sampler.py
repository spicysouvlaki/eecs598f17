''' sampler.py
describes the abstract class for samplers to implement
'''

from abc import ABC, abstractmethod
import pandas as pd
import math
from heapq import heappop, heappush
from numbers import Number

class DataPoint:
    def __init__(self, ndarray, rsp=0):
        self.ndarray = ndarray
        self.rsp = rsp

    def getPoint(self): return self.ndarray

    def get_rsp(self): return self.rsp

    def set_rsp(self, rsp): self.rsp = rsp

    def addto_rsp(self, amount_to_add): self.rsp += amount_to_add

    def removefrom_rsp(self, amount_to_remove): self.rsp += amount_to_remove

    def __str__(self):
        return str(self.ndarray) + ", rsp: " + str(self.rsp)

    def __getitem__(self, index):
        return self.ndarray[index]

    def __gt__(self, other):
        return self.rsp > other.rsp

    def __lt__(self, other):
        return self.rsp < other.rsp

class DataSet:
    def __init__(self, dataset=[], usePQ=False):
        self.dataset = [DataPoint(x) for x in dataset]
        self.usePQ = usePQ
        if usePQ:
            self.usePQ = True
            self.switch_to_prioirity_dataset()

    def __add__(self, other):
        self.usePQ = self.usePQ or other.usePQ
        self.dataset.extend(other.dataset)
        return self

    def switch_to_prioirity_dataset(self):
        if len(self.dataset):
            raise TypeError("cannot convert a linear DataSet to a priority based dataset")

        self.usePQ = True # in case this isn't being called from the constructor

        # remove linear/simple array based funcationality
        self.__delitem__ = None
        self.__setitem__ = None

        # add priority based functionality
        self.insert_new_point = self._insert_new_point_pq
        self.remove = self._remove_pq
        return self

    def deheapify(self):
        if self.usePQ:
            stripped_dataset = []
            for pair in self.dataset:
                assert isinstance(pair[1], DataPoint) # type check to avoid any weird errors
                stripped_dataset.append(pair[1])
            self.dataset = stripped_dataset
        return self

    def __len__(self):
        return len(self.dataset)

    def insert_new_point(self, ndarray, rsp=0):
        self.dataset.append(DataPoint(ndarray, rsp))

    def _insert_new_point_pq(self, ndarray, rsp):
        heappush(self.dataset, (1/rsp, DataPoint(ndarray, rsp)))

    def remove(self, index):
        del self.dataset[index]

    def getPoints(self):
        return [x.getPoint() for x in self.dataset]

    def __getitem__(self, index):
        return self.dataset[index]

    def __setitem__(self, index, value):
        self.dataset[index] = value

    def __delitem__(self, index):
        del self.dataset[index]

    def _remove_pq(self):
        self._fail_if_not_pq()
        heappop(self.dataset)

    def _fail_if_not_pq(self):
        if not self.usePQ: raise TypeError("Cannot call a PQ func on a non-PQ based DataSet")

    def isUsingPQmode(self):
        return self.usePQ


class AbstractHoppingSampler(ABC):
    def __init__(self, sample_size, column_range, eta):
        """
        :param sample_size: <Int>  length on input data
        :param column_range: <(Int, Int)> tuple of integers describing the start and end columns to calculate proximity on
        :param eta: <Float> normalization parameter in proximity
        """

        self.sample_size = sample_size
        self.column_range = list(range(column_range[0], column_range[1]))  # turn generator into list to save compute time in proximity loop
        self.eta = eta
        self.etaSq = eta ** 2

    def insert_and_updateSet_naive(self, sample_set, point):
        """
        takes in a dict of (points, responsibility) and a point, returns the point's responsibility and updates each items responsibility
        :param sample_set: DataSet
        :param point: numpy.ndarray
        :return: (updated) DataSet
        """
        rsp = 0  # rsp short for responsibility
        for x in range(len(sample_set)):
            rsp_at_x = self.proximity(sample_set[x].getPoint(), point)
            sample_set[x].addto_rsp(rsp_at_x)
            rsp += rsp_at_x
        sample_set.insert_new_point(point, rsp)
        return sample_set

    def proximity(self, p1, p2):
        '''represented by k~ in the paper '''
        if isinstance(p1, Number) or isinstance(p2, Number):
            raise ValueError("something weird is getting passed in for p1, p2...")
        vec = [abs(p1[i] - p2[i]) for i in self.column_range]
        sq_magnitude = sum([abs(p1[i] - p2[i]) for i in self.column_range])
        return math.e ** (-1 * sq_magnitude / self.etaSq)

    @abstractmethod
    def sample(self, window):
        pass

    def persist_sample_set(self, sample_set, filename, columns, num_windows):
        """
        :param sample_set: DataSet
        :param filename: <String> filename to write final window
        :param columns: <list(String)> list of strings to label dataset as
        :return: None
        """
        full_columns = list(columns).copy()
        full_columns.extend(["window_number"])
        df = pd.DataFrame(columns=full_columns)

        for num_window in range(num_windows):
            window = sample_set[num_window].getPoints()
            window_df = pd.DataFrame(columns=df.columns)
            for point in window:
                point_dct = dict()
                for x in range(len(columns)): point_dct[columns[x]] = point[x]
                point_dct['window_number'] = num_window
                window_df = window_df.append(point_dct, ignore_index=True)
            df = df.append(window_df)

        self.DataFrame = df
        df.to_csv(filename, index=False)
