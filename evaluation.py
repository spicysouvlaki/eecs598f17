''' evaluation.py
misc file containing metrics needed to evaluate input dataset
'''

import pandas as pd

class Evaluator:
    def __init__(self, sample_set, sampler):
        """
        :param sample_set: dict(window_num <int>, sample (list(Points)))
        :param sampler: inherits from AbstractSampler
        """
        self.sampler = sampler
        self.sample_set = sample_set
        self.run_metrics()

    def run_metrics(self):
        losses = [[window_num, self.Loss(self.sample_set[window_num])] for window_num in range(len(self.sample_set))]
        self.loss_df = pd.DataFrame(losses, columns=['window_num', 'loss'])
        return

    def Loss(self, sample):
        def pointLoss(current_point):
            point_cost = 0
            for point in sample:
                point_cost += self.sampler.proximity(current_point, point)
            return point_cost
        return sum([pointLoss(point) for point in sample])


    def _averageInvResponsibility(self):
        if self.averageInvResponsibilityValue:
            return self.averageInvResponsibilityValue
        window_range = range(len(self.sample_set) - 1)
        total_rsp = 0
        for window in window_range:
            wrsp = 0
            for p1 in window:
                for p2 in window:
                    wrsp += self.sampler.proximity(p1, p2)
            total_rsp += wrsp
        return 1 / math.log(total_rsp / len(self.sample_set))


    def save(self, filename):
        self.loss_df.to_csv(filename, index=False)
        return

    def __str__(self):
        l = ["window_number,loss,sampler"]
        for i in range(len(self.loss)):
            l.append("{},{},{}".format(i, self.loss[i], self.sampler))
        return '\n'.join(l)
