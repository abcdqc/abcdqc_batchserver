
# coding: utf-8

# In[29]:


import pandas as pd
import json
from scipy.stats import norm
import numpy as np
from scipy import stats
import seaborn as sns
import statsmodels.api as sm
import time
from itertools import product


# In[30]:


# load data frame from csv
df = pd.read_csv("/abcdqc_data/batchserver/output/df.csv")
df.columns = df.columns.str.replace('.', '__')


# In[31]:


# generate summary measures for all iqms
t1wiqms = ['cjv', 'cnr', 'efc', 'fber', 'fwhm_avg',
       'fwhm_x', 'fwhm_y', 'fwhm_z', 'icvs_csf', 'icvs_gm', 'icvs_wm',
       'inu_med', 'inu_range', 'qi_1', 'qi_2', 'rpve_csf', 'rpve_gm',
       'rpve_wm', 'size_x', 'size_y', 'size_z', 'snr_csf', 'snr_gm',
       'snr_total', 'snr_wm', 'snrd_csf', 'snrd_gm', 'snrd_total', 'snrd_wm',
       'spacing_x', 'spacing_y', 'spacing_z', 'summary_bg_k', 'summary_bg_mad',
       'summary_bg_mean', 'summary_bg_median', 'summary_bg_n',
       'summary_bg_p05', 'summary_bg_p95', 'summary_bg_stdv', 'summary_csf_k',
       'summary_csf_mad', 'summary_csf_mean', 'summary_csf_median',
       'summary_csf_n', 'summary_csf_p05', 'summary_csf_p95',
       'summary_csf_stdv', 'summary_gm_k', 'summary_gm_mad', 'summary_gm_mean',
       'summary_gm_median', 'summary_gm_n', 'summary_gm_p05', 'summary_gm_p95',
       'summary_gm_stdv', 'summary_wm_k', 'summary_wm_mad', 'summary_wm_mean',
       'summary_wm_median', 'summary_wm_n', 'summary_wm_p05', 'summary_wm_p95',
       'summary_wm_stdv', 'tpm_overlap_csf', 'tpm_overlap_gm',
       'tpm_overlap_wm', 'wm2max']
t2wiqms = ['cjv', 'cnr', 'efc', 'fber', 'fwhm_avg',
       'fwhm_x', 'fwhm_y', 'fwhm_z', 'icvs_csf', 'icvs_gm', 'icvs_wm',
       'inu_med', 'inu_range', 'qi_1', 'qi_2', 'rpve_csf', 'rpve_gm',
       'rpve_wm', 'size_x', 'size_y', 'size_z', 'snr_csf', 'snr_gm',
       'snr_total', 'snr_wm', 'snrd_csf', 'snrd_gm', 'snrd_total', 'snrd_wm',
       'spacing_x', 'spacing_y', 'spacing_z', 'summary_bg_k', 'summary_bg_mad',
       'summary_bg_mean', 'summary_bg_median', 'summary_bg_n',
       'summary_bg_p05', 'summary_bg_p95', 'summary_bg_stdv', 'summary_csf_k',
       'summary_csf_mad', 'summary_csf_mean', 'summary_csf_median',
       'summary_csf_n', 'summary_csf_p05', 'summary_csf_p95',
       'summary_csf_stdv', 'summary_gm_k', 'summary_gm_mad', 'summary_gm_mean',
       'summary_gm_median', 'summary_gm_n', 'summary_gm_p05', 'summary_gm_p95',
       'summary_gm_stdv', 'summary_wm_k', 'summary_wm_mad', 'summary_wm_mean',
       'summary_wm_median', 'summary_wm_n', 'summary_wm_p05', 'summary_wm_p95',
       'summary_wm_stdv', 'tpm_overlap_csf', 'tpm_overlap_gm',
       'tpm_overlap_wm', 'wm2max']
boldiqms = ['dummy_trs', 'dvars_nstd',
       'dvars_std', 'dvars_vstd', 'efc', 'fber', 'fd_mean', 'fd_num',
       'fd_perc', 'fwhm_avg', 'fwhm_x', 'fwhm_y', 'fwhm_z', 'gcor', 'gsr_x',
       'gsr_y', 'provenance__settings__fd_thres', 'size_t', 'size_x', 'size_y',
       'size_z', 'snr', 'spacing_tr', 'spacing_x', 'spacing_y', 'spacing_z',
       'summary_bg_k', 'summary_bg_mad', 'summary_bg_mean',
       'summary_bg_median', 'summary_bg_n', 'summary_bg_p05', 'summary_bg_p95',
       'summary_bg_stdv', 'summary_fg_k', 'summary_fg_mad', 'summary_fg_mean',
       'summary_fg_median', 'summary_fg_n', 'summary_fg_p05', 'summary_fg_p95',
       'summary_fg_stdv', 'tsnr']

mod_dict = {'T1w': t1wiqms, 'T2w': t2wiqms, 'bold': boldiqms}


# In[32]:


splitvars = [ 'bids_meta__Manufacturer', 'bids_meta__ManufacturersModelName', 'bids_meta__TaskName', 'qc_ok', 'gender']
split_uniques = [list(df[sv].unique()) + ['all'] for sv in splitvars]


# In[69]:


def kdegen(dataframe, x_list): 
    # returns x and y as tuples
    x_list = x_list[~np.isnan(x_list)] # remove NaNs
    kernel = stats.gaussian_kde(x_list)
    x = np.linspace(x_list.min(), x_list.max(), num=1000)
    y = kernel.evaluate(x)
    return(list(zip(x, y)))

def kdetuples(dataframe, iqms):
    kdedict = {}
    for iqm in iqms:
        x_list = dataframe[iqm]
        if x_list.nunique() >= 2:
            kdedict[iqm] = {}
            kdedict[iqm]['kde'] = kdegen(dataframe, x_list)
            kdedict[iqm]['boxplot'] = {'quartiles': list(x_list.quantile([0.25, 0.5, 0.75]).astype(float)),
                                       'extremes': [float(x_list.min()), float(x_list.max())]}
        kdedict['n_subs'] = int(dataframe.bids_meta__subject_id.nunique())
        kdedict['n_scans'] = int(dataframe.provenance__md5sum.nunique())
    return(kdedict)


# In[ ]:


def subsetdf(dataframe, varname, varval):
    return dataframe.loc[dataframe[varname] == varval]

# Logic to identify the n of the subgroup
def subgroupsize(subdf):
    return(len(subdf.iloc[:,1]))

def writejson(data, filename):
    with open('/abcdqc_data/batchserver/output/v0.1/' + filename, 'w') as outfile:
        json.dump(data, outfile)

def get_combined_index(svs, uvs, df):
    combo_ind = df.provenance__md5sum.notnull()
    for sv, uv in zip(svs, uvs):
        if uv is not 'all':
            if pd.notnull(uv):
                combo_ind = combo_ind & (df[sv] == uv)
            else:
                combo_ind = combo_ind & (df[sv].isnull())
    return combo_ind

df_mods = [ 'T1w', 'T2w', 'bold']

# divide df into 3 sets based on modality because they contain different sets of IQMs
start_time = time.time()
for mrimode in df_mods:
    cols = mod_dict[mrimode] + splitvars + ['provenance__md5sum', 'bids_meta__subject_id']
    mode_df = df.loc[df.bids_meta__modality == mrimode, cols ]
    combo_inds = []
    combos = list(product(*split_uniques))
    for ci, uvs in enumerate(combos):
        combo_ind = get_combined_index(splitvars, uvs, mode_df)
        subdf = mode_df[combo_ind]
        if len(subdf) >= 100:
            json_name = f'Modality-{mrimode}___' + '___'.join(['-'.join([str(name_map[sv]),str(uv)]) for sv, uv in zip(splitvars, uvs)]) + '.json'
            writejson(kdetuples(subdf, mod_dict[mrimode]), json_name)
        if ci % 100 == 0:
            print('finished', end='', flush=True)
            print(f' {ci}', end=', ', flush=True)
    print(f"finished {mrimode}")
            


# In[34]:


name_map = {'bids_meta__Manufacturer':'Manufacturer',
            'bids_meta__ManufacturersModelName': 'Model',
            'bids_meta__TaskName': 'Task',
            'qc_ok':'QC',
            'gender':'Sex'}


# In[61]:


foo = kdetuples(subdf, mod_dict[mrimode])


# In[33]:


#     # use a dictionary to allow different variable names as mentioned here:
#     # https://stackoverflow.com/questions/6181935/how-do-you-create-different-variable-names-while-in-a-loop
#     d = dict() # reset dictionary to only have one entry
#     d[mrimode] = 
#     # d.values()[0] returns the first entry in the dictionary
#     mode_df = list(d.values())[0]
#     # iterate over all 3 subsets
#     sv_iter(mode_df, mrimode)
# print(time.time() - start_time, " seconds")


# In[11]:


splitvars = [ 'bids_meta__Manufacturer', 'bids_meta__ManufacturersModelName', 'bids_meta__modality', 'bids_meta__TaskName', 'qc_ok', 'gender']
split_uniques = [list(df[sv].unique()) + ['all'] for sv in splitvars]

def get_combined_index(svs, uvs, df):
    combo_ind = df.provenance__md5sum.notnull()
    for sv, uv in zip(svs, uvs):
        if uv is not 'all':
            if pd.notnull(uv):
                combo_ind = combo_ind & (df[sv] == uv)
            else:
                combo_ind = combo_ind & (df[sv].isnull())
    return combo_ind

combo_inds = []
combos = list(product(*split_uniques))
for uvs in combos:
    combo_ind = get_combined_index(svs, uvs, df)
    subdf = df[combo_ind]
    if len(subdf) >= 100:
        
    break

