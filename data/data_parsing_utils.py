import os
import pandas as pd

def getSamplerType(name):
    if 'Will' in name: return "WillsSampler"
    if "andom" in name: return "Random"
    if "Vaas" in name: return "IdealSampler"

def parseFilename(name):
    features = dict()
    features['Sampler'] = getSamplerType(name)
    features['Window Size'] = getFeature(name, 'w=')
    features['Sample Size'] = getFeature(name, 'sample_size=')
    features['Parallel Count'] = getFeature(name, 'parallel_count=')
    features['Cluster Count'] = getFeature(name, 'cluster_count=')
    data = pd.read_csv(name)
    features['Average Loss'] = data['loss'].mean()
    return features

def getFeature(filename, ft):
    pos = filename.find(ft)
    if pos < 0: return 0
    return int(filename[pos + len(ft) : filename.find('.', pos)])

def loadandparasedata():
    df = pd.DataFrame(columns=['Sampler', 'Window Size', 'Sample Size', 'Parallel Count', 'Cluster Count', 'Average Loss'])
    filenames = [x for x in os.listdir('.') if ".csvevaluator.csv" in x]
    for filename in filenames:
        features = parseFilename(filename)
        df = df.append(features, ignore_index=True)
    return df


if __name__ == '__main__':
    print("running as main")
    df = loadandparasedata()
    df.to_csv('./master_data.csv')
