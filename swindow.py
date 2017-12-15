''' swindow.py:
this file contains functions that will read from csv or other input source and return data window generators

'''

import pandas as pd


def buildFromCSV(filename, window_size, timestamp_col):
    df = pd.read_csv(filename)
    return HoppingWindow(window_size, df, timestamp_col), df.columns

def is_window_big_enough(start, end, input_stream, col, window_length):
    if end >= len(input_stream): return True
    elif input_stream[col][start] + window_length < input_stream[col][end]: return True
    else: return False

class HoppingWindow:
    def __init__(self, window_size, input_object, timestamp_col):
        self.timestamp_col = timestamp_col
        self.window_size = window_size # size of the window in seconds
        self.stream_length = len(input_object)
        self.input_object = input_object

    def startWindow(self):
        return self.hopper()

    def hopper(self):
        start = 1
        end = 1
        while end < len(self.input_object):
            while not is_window_big_enough(start, end, self.input_object, self.timestamp_col, self.window_size):
                end += 10
            yield self.input_object[start:end].itertuples(index=False)
            start = end

        return None
