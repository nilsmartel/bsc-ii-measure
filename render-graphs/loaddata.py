import numpy as np
import pandas as pd
import os

# loads up data organized per algorithm
# dict(
#       algorithm =>
#           dataframe)
def loaddata(dir):
    data = dict()

    files = os.listdir(dir)
    files.sort()
    for path in files:
        frame = pd.read_csv(dir + "/" + path)
        tablename, algo, kind = parsename(path)

        if kind == 'retr':
            retrieval_ns = np.average(frame)
            data[(tablename, algo)]['retrieval_ns'] = retrieval_ns
        else:
            data[(tablename, algo)] = frame

    data_per_algos = dict()
    for (tablename, algo), value in data.items():
        if algo in data_per_algos:
            value['source'] = tablename
            data_per_algos[algo].append(value)
        else:
            value['source'] = tablename
            data_per_algos[algo] = value

    return data_per_algos


def parsename(path: str):
    filename = path.rsplit("/", maxsplit=1).pop()
    _depr_size, rest = filename.split("-", maxsplit=1)

    tablename_algo, rest = rest.rsplit("-", maxsplit=1)
    tablename, algo = tablename_algo.rsplit("-", maxsplit=1)
    kind = rest.replace("-", "").replace(".csv", "")

    return tablename, algo, kind


if __name__ == "__main__":
    tablename, algo, kind = parsename("0-open_ai-ns-retr.csv")

    assert(tablename == "open_ai")
    assert(algo == "ns")
    assert(kind == "retr")

