''' VAAS_Interchange.py
implement VAAS Interchange algorithm for sliding and hopping windows
each function/class should take in a window and return the sample set
'''
from Sampler import AbstractHoppingSampler, DataSet, DataPoint
import numpy as np

class VaasInterchangeHopping(AbstractHoppingSampler):
    ''' most naive impl of Visualization Aware Streaming Algorithm I could make, yields *near* optimal results'''
    def sample(self, window):
        """
        sample input window, return sampled dataset
        :param window: list of points
        :return: reduced list of points
        """
        sample_set = DataSet()
        for point in window:
            sample_set = self._grow(sample_set, np.array(point, dtype=np.float64))
        return sample_set

    def _shrink(self, sample_set):
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
        self.insert_and_updateSet_naive(sample_set, point)
        if len(sample_set) >= self.sample_size:
            sample_set = self._shrink(sample_set)
        return sample_set

    def __str__(self):
        return "VaasInterchangeHopping_Sampler"

    def __repr__(self): return self.__str__()