
# coding: utf-8

# In[4]:


import pandas as pd
import json
import numpy as np
from matplotlib import pyplot as plt
from scipy import stats
import seaborn as sns


# In[9]:


# read data from the JSON created by kde-gen

def parse_json(filename):
    with open(filename) as f:
        return(json.load(f))

dat = parse_json('/abcdqc_data/batchserver/output/v0.1/Modality-bold___Manufacturer-Siemens___Model-Prisma_fit___Task-sst___QC-nan___Sex-nan.json')


# In[10]:


type(dat)


# In[13]:


dat.keys()


# In[14]:


dat['n_subs']


# In[15]:


dat['n_scans']


# In[18]:


dat['tsnr']['boxplot']


# In[19]:


testkde = dat['tsnr']['kde']


# In[20]:


type(testkde)


# In[21]:


testkde[:5]


# In[23]:


# https://stackoverflow.com/questions/12142133/how-to-get-first-element-in-a-list-of-tuples
x = [i[0] for i in testkde]
y = [i[1] for i in testkde]


# In[24]:


plt.scatter(x, y)


# In[ ]:




