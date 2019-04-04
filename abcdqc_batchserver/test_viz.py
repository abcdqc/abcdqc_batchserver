import pandas as pd
import json
import numpy as np
from matplotlib import pyplot as plt
from scipy import stats
import seaborn as sns

# read data from the JSON created by kde-gen

def parse_json(filename):
    with open(filename) as f:
        return(json.load(f))

dat = parse_json('/abcdqc_data/batchserver/output/v0.1/Modality-bold___Manufacturer-Siemens___Model-Prisma_fit___Task-sst___QC-nan___Sex-nan.json')

type(dat)

dat.keys()

dat['n_subs']

dat['n_scans']

dat['tsnr']['boxplot']

testkde = dat['tsnr']['kde']

type(testkde)

testkde[:5]

# https://stackoverflow.com/questions/12142133/how-to-get-first-element-in-a-list-of-tuples
x = [i[0] for i in testkde]
y = [i[1] for i in testkde]

plt.scatter(x, y)
