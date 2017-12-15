''' random_sampler.py
random sampling for hopping windows
each function/class should take in a window and return the sample set
'''
from Sampler import AbstractHoppingSampler, DataSet, DataPoint
import numpy as np

class RandomHoppingSampler(AbstractHoppingSampler):
    ''' most naive impl of Visualization Aware Streaming Algorithm I could make, yields *near* optimal results'''
    def sample(self, window):
        """
        sample input window, return sampled dataset
        :param window: list of points
        :return: reduced list of points
        """
        sample_set = list(window)
        random_selection = []
        sample_size = min(self.sample_size, len(sample_set))
        for idx in np.random.choice(len(sample_set), size=sample_size, replace=False):
            random_selection.append(sample_set[idx])
        # determine responsiblities
        sample_set = DataSet([])

        for point1 in random_selection:
            rsp = 0
            for point2 in random_selection:
                rsp += self.proximity(point1, point2)
            sample_set.insert_new_point(point1, rsp)
        return sample_set

    def __str__(self):
        return "Random_Sampler"

    def __repr__(self): return self.__str__()