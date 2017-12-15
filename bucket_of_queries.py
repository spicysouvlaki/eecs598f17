"""
a function that runs a bucket of queries using pandas dataframes against whatever data is passed in
"""
from time import time
import pandas as pd

def loadCSVandRunQuery(filename):
    df = pd.read_csv(filename)
    return runQueries(df)

def runQueries(dataframe):
    assert isinstance(dataframe, pd.DataFrame)
    start_simple = time()
    _runSimple(dataframe)
    end_simple = time()

    start_complex = time()
    _runComplex(dataframe)
    end_complex = time()

    return {"Simple_Time" : end_simple - start_simple, "Complex_Time" : end_complex - start_complex}

def _runSimple(dataframe):
    # find n-largest by column from the median
    l = []
    for col in dataframe.columns:
        col_median = dataframe[col].median()
        l.append(dataframe.nlargest(10, col))

    return l

def _runComplex(dataframe):
    x = set(
        dataframe[(dataframe['Amount'] > dataframe["Amount"].median()) & (dataframe['V1'] < 0) | (dataframe['V2'].median() < dataframe['V2'] & dataframe['V3'] > 0)],
        dataframe[(dataframe['Amount'] > dataframe['Amount'].median()) & (dataframe['V1'] < 0) | (dataframe['V2'].median() < dataframe['V2'] & dataframe['V3'] > 0)],
        dataframe[(dataframe['V3'] > dataframe["V3"].median()) & (dataframe['V1'] < 1) | (dataframe['V2'].mean() < dataframe['V2'] + 1 & dataframe['V3'] + 2 > 0)],
        dataframe[(dataframe['Amount'] > dataframe["Amount"].median()) & (dataframe['V1'] < 0) | (dataframe['V2'].median() < dataframe['V2'] & dataframe['V3'] > 0)],
        dataframe[(dataframe['Amount'] > dataframe["Amount"].median()) & (dataframe['V1'] < 0) | (dataframe['V11'].median() < dataframe['V12'] & dataframe['V9'] > 0)],
        dataframe[(dataframe['Amount'] > dataframe["Amount"].median()) & (dataframe['V1'] < 0) | (dataframe['V12'].median() < dataframe['V22'] & dataframe['V7'] > 0)],
        dataframe[(dataframe['Amount'] > dataframe["Amount"].median()) & (dataframe['V1'] < 0) | (dataframe['V21'].mean() < dataframe['V23'] & dataframe['V5'] > 0)],
        dataframe[(dataframe['Amount'] > dataframe["Amount"].median()) & (dataframe['V1'] < 0) | (dataframe['V14'].median() < dataframe['V14'] & dataframe['V28'] > 0)],
        dataframe[(dataframe['Amount'] > dataframe["Amount"].median()) & (dataframe['V1'] < 0) | (dataframe['V5'].median() < dataframe['V5'] & dataframe['V13'] > 0)],
    )

    return [len(y) for y in x]