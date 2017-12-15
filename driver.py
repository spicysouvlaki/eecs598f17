''' driver to import data and run experiments
'''

from time import time
import pandas as pd
from swindow import buildFromCSV
from VAAS_Interchange import VaasInterchangeHopping
from evaluation import Evaluator
from WillsSampler import WillsSampler, loadClusters
from random_sampler import RandomHoppingSampler
import pickle
from bucket_of_queries import loadCSVandRunQuery


eta = 100

def _persistentFileName(samplerName, filename, windowSize, sample_size, evaluator=False):
    if not evaluator:
        return "{}.sampler={}.w={}.sample_size={}.csv".format(filename, samplerName, str(windowSize), sample_size)
    else:
        return "{}.sampler={}.w={}.sample_size={}.evaluator.csv".format(filename, samplerName, str(windowSize),sample_size)

def evaluate_sampler(sampler, windows):
    """
    run the passed in sampler on the passed in window, returns an Evaluator(sampler)
    :param sampler: type of sampler to run window set against
    :param windows: an instance of AbstractHoppingWindow
    :return: Evaluator
    """
    return None


def build_ideal_window_hopping_set(windowSize, AbstractBaseSampler, filename='./data/creditcard.csv', max_window_ct=3):
    filename = './data/creditcard.csv'
    sample_sizes = [30,40,60]
    for sample_size in sample_sizes:
        hopper, column_names = buildFromCSV(filename, windowSize, "Time")
        windows = hopper.hopper()
        sampler = AbstractBaseSampler(sample_size, (2, 29), eta) # sample size, column range, eta
        samples = dict()
        num_windows = 0
        for window in windows:
            samples[num_windows] = sampler.sample(window)
            num_windows += 1
            if num_windows > max_window_ct: break

        storage_filename = _persistentFileName(str(sampler),filename,windowSize,sample_size)
        # store dataset and return evaluation metrics on it
        sampler.persist_sample_set(samples, storage_filename, column_names, num_windows)
        e = Evaluator(samples, sampler)
        e.save(storage_filename + "evaluator.csv")
    return


def evaluateWillsSamplerParallel(windowSize,filename='./data/creditcard.csv',max_window_ct=3):
    parallel_counts = [1,2,4,8,16,32]
    filename = './data/creditcard.csv'
    sample_sizes = [30,40,60]
    for sample_size in sample_sizes:
        for parallel_count in parallel_counts:
            hopper, column_names = buildFromCSV(filename, windowSize, "Time")
            windows = hopper.hopper()
            sampler = WillsSampler(sample_size, (2, 29), eta, parallel_count=parallel_count)
            samples = dict()
            num_windows = 0
            for window in windows:
                samples[num_windows] = sampler.sample(window).deheapify()
                num_windows += 1
                if num_windows > max_window_ct: break

            storage_filename = sampler.persistent_filename(filename, windowSize)
            # store dataset and return evaluation metrics on it
            sampler.persist_sample_set(samples, storage_filename, column_names, num_windows)
            e = Evaluator(samples, sampler)
            e.save(storage_filename + "evaluator.csv")
    return


def evaluateWillsSamplerClusters(windowSize,filename='./data/creditcard.csv',max_window_ct=3):
    parallel_counts = [1,4,16]
    cluster_choices = [1, 6, 11, 21, 31, 51]
    cluster_centers_collection = loadClusters(cluster_choices)
    filename = './data/creditcard.csv'
    sample_sizes = [30,40,60,100]
    for sample_size in sample_sizes:
        for num_centers, cluster_centers in cluster_centers_collection.items():
            for parallel_count in parallel_counts:
                hopper, column_names = buildFromCSV(filename, windowSize, "Time")
                windows = hopper.hopper()
                sampler = WillsSampler(sample_size, (2, 29), eta, parallel_count=parallel_count, cluster_centers=cluster_centers)
                samples = dict()
                num_windows = 0
                for window in windows:
                    samples[num_windows] = sampler.sample(window).deheapify()
                    num_windows += 1
                    if num_windows > max_window_ct: break

                storage_filename = sampler.persistent_filename(filename, windowSize)
                # store dataset and return evaluation metrics on it
                sampler.persist_sample_set(samples, storage_filename, column_names, num_windows)
                e = Evaluator(samples, sampler)
                e.save(storage_filename + "evaluator.csv")
    return


def timestamp():
    import time
    local = time.localtime()
    return "{}, {}:{}:{}".format(local.tm_mday, local.tm_hour, local.tm_min, local.tm_sec)

def getFullDataFileNames():
    from os import listdir
    def isForbidden(filename):
        for x in ['.DS_Store', '.pickle', 'pkl']:
            if x in filename:
                return True # is forbidden
        return False

    return ['./data/' + filename for filename in listdir('./data/') if not isForbidden(filename)]


def buildFullDataSets():
    test_window_sizes = [30, 100, 200, 300, 500]
    # proposed sampler test
    for w in test_window_sizes:
        evaluateWillsSamplerParallel(w)
        evaluateWillsSamplerClusters(w)
        print("{} | completed {} (hopping) for {} window size".format(timestamp(), WillsSampler, w))
        print(timestamp())

    # comparing to other samplers
    samplers = [VaasInterchangeHopping, RandomHoppingSampler]
    for sampler in samplers:
        for w in test_window_sizes:
            build_ideal_window_hopping_set(w, sampler)
            print("{} | completeted {} (hopping) for {} window size".format(timestamp(), sampler, w))
    return

def timeSampler(Sampler, windows, ct=10):
    sample_set = dict()
    start = time()
    for i in range(ct):
        try:
            window = next(windows)
            sample_set[i] = Sampler.sample(window)
        except StopIteration:
            break
    end = time()
    tdelta = end - start
    return tdelta, len(sample_set) # returns the length of the sample set because I don't want the for loop to get optimized out

def print_sampler_timinig(sampler, time, window_size):
    other_characteristics = ["{}={}.".format("sample_size",sampler.sample_size)]
    if isinstance(sampler, WillsSampler):
        other_characteristics.append("{}={}.".format('parallel_count', sampler.parallel_count))
        if sampler.use_clusters:
            other_characteristics.append("{}={}.".format('cluster_centers', len(sampler.cluster_centers)))

    out = "{}.w={}.{} time: {}".format(sampler, window_size, other_characteristics, time)
    print(out)
    return out

def timeAllSamplers(test_window_sizes, sample_sizes=[30,40,60]):
    # hold all sampler to be timed
    all_samplers = []

    # add WillsSamplers first, they need: windowSize, (2, 29), eta, parallel_count = parallel_count, cluster_centers = cluster_centers
    parallel_counts = [1, 2, 4, 8, 16]
    cluster_choices = [1, 6, 11, 16, 21, 31, 41, 51]
    cluster_centers_collection = loadClusters(cluster_choices)
    for sample_size in sample_sizes:
        for parallel_count in parallel_counts:
            for cluster_centers in cluster_centers_collection.values():
                all_samplers.append(WillsSampler(sample_size, (2, 29), eta, parallel_count = parallel_count, cluster_centers = cluster_centers))
            all_samplers.append(WillsSampler(sample_size, (2, 29), eta, parallel_count=parallel_count, cluster_centers=[]))

    # add the baseline samplers
    for sample_size in sample_sizes:
        all_samplers.append(VaasInterchangeHopping(sample_size, (2, 29), eta))

    filename = './data/creditcard.csv'
    timings = dict()
    for windowSize in test_window_sizes:
        for sampler in all_samplers:
            hopper, column_names = buildFromCSV(filename, windowSize, "Time")
            timetaken, _ = timeSampler(sampler, hopper.hopper(), ct=5)
            name = print_sampler_timinig(sampler, timetaken, windowSize)
            timings[name] = timetaken

    pickle.dump(timings, open('./data/timings.' + str(time()) + '.pickle'))

def testQueries():
    all_query_timings = {}
    filenames = getFullDataFileNames()
    for filename in filenames:
        all_query_timings[filename] = loadCSVandRunQuery(filename)
    s = ["filename,simple_time,complex_time"]
    s.extend(["{},{},{}".format(filename, time["Simple_Time"], time["Complex_Time"]) for filename, time in all_query_timings.items()])
    with open('./data/file_timings.csv', 'w') as fp:
        fp.write('\n'.join(s))

if __name__ == '__main__':
    print("starting all experiments, this might take a few minutes...")
    print("current time:", timestamp())
    buildFullDataSets()
    print("current time:", timestamp())
    # test query performance
    print("current time:", timestamp())
    timeAllSamplers([30, 50, 100, 150, 200, 300, 400, 500])
    print("finished doing timings!! starting to build datasets")
    print("...finished running!")

