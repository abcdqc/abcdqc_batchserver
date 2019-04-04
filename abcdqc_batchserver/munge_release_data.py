from pathlib import Path
import pandas as pd

def load_abcd_table(path):
    table = pd.read_csv(path, skiprows=[1], header=0, sep='\t')
    labels = pd.read_csv(path, nrows=1, header=0, sep='\t')
    labels = labels.T.reset_index().rename(columns={'index':'name', 0:'doc'})
    return table, labels

release_dir = Path('/abcdqc_data/releases/1.1/ABCDstudyDEAP')

# get the list of subjects in the most recent release
# We're assuming that all subjects are in the site table
site_df = pd.read_csv(release_dir / 'abcd_lt01.txt',
                      skiprows = [1],
                      header = 0,
                      sep='\t')

site_df['bids_meta__subject_id'] = site_df.subjectkey.str.replace('_','')

json_dat = pd.read_csv('/abcdqc_data/batchserver/output/df.csv')
json_dat.columns = json_dat.columns.str.replace('.', '__')

to_merge = site_df.loc[:,['interview_age', 'gender', 'bids_meta__subject_id']]
mriqc_merge = json_dat.merge(to_merge, how='left', on='bids_meta__subject_id', indicator=True)
assert mriqc_merge.groupby('_merge').provenance__md5sum.count()['right_only'] == 0
assert mriqc_merge.groupby('_merge').provenance__md5sum.count()['both'] != 0
assert mriqc_merge.groupby('_merge').provenance__md5sum.count()['left_only'] != 0

# Get the FreesurferQC
fsqc, fsqclbl = load_abcd_table(release_dir / 'freesqc01.txt')
mriqc, mriqclbl = load_abcd_table(release_dir / 'mriqc02.txt')
mriqc2, mriqclbl = load_abcd_table(release_dir / 'mriqcp202.txt')
midbeh, _ = load_abcd_table(release_dir / 'abcd_mid02.txt')
nbbeh, _ = load_abcd_table(release_dir / 'abcd_mrinback02.txt')
sstbeh, _ = load_abcd_table(release_dir / 'abcd_sst02.txt')
mrfind, _ = load_abcd_table(release_dir / 'abcd_mrfindings01.txt')

image_tbls = ["abcd_smrip101", "abcd_smrip201", "abcd_dti_p101", "abcd_dti_p201",
 "mri_rsi_p102", "mri_rsi_p202", "abcd_midr1bwp101", "abcd_midr1bwp201",
 "midr2bwp101", "midr2bwp201", "mrisstr1bw01", "mrisstr2bw01", "nbackr101",
 "nbackr201", "abcd_mrirstv02", "midaparc02", "midaparcp202", 
 "abcd_midasemp101", "abcd_midasemp201", "abcd_midsemp101",
 "abcd_midsemp201", "abcd_midr2semp101", "abcd_midr2semp201",
 "mrisst02", "mrisstsem01", "mrisstr1sem01", "mrisstr2bwsem01",
 "nback_bwroi02", "nbackallsem01", "nbackr1sem01", "nbackr2sem01",
 "dmriqc01", "abcd_mid02", "abcd_sst02", "abcd_mrinback02", "mribrec02",
 "abcd_mrfindings01", "mriqc02", "mriqcp202", "freesqc01"]
non_deskian_tables = ["abcd_mrirstv02", "dmriqc01", "abcd_mid02", "abcd_sst02", "abcd_mrinback02", "mribrec02",
 "abcd_mrfindings01", "mriqc02", "mriqcp202", "freesqc01"]
deskiab_tbls = [it for it in image_tbls if it not in non_deskian_tables]
imgtbls = {}
imglbls = []
for itn in deskiab_tbls:
    imgtbl, imglbl = load_abcd_table(release_dir / (itn + '.txt'))
    imglbl['source_file'] = itn
    imgtbls[itn] = imgtbl
    imglbls.append(imglbl)

longtbl = fsqc

longtbl = longtbl.merge(mriqc.loc[:, ['subjectkey', 'interview_date', 'eventname', 'iqc_t2_ok_ser', 'iqc_mid_ok_ser']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)

longtbl = longtbl.merge(mriqc2.loc[:, ['subjectkey', 'interview_date', 'iqc_sst_ok_ser', 'iqc_nback_ok_ser']],
                how='left',
                on=['subjectkey','interview_date'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)

longtbl = longtbl.merge(midbeh.loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_mid_beh_perform.flag']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)
longtbl = longtbl.merge(nbbeh.loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_nback_beh_perform.flag']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)
longtbl = longtbl.merge(sstbeh.loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_sst_beh_perform.flag']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)

longtbl = longtbl.merge(imgtbls['midaparc02'].loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_mid_all_beta_dof']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)
longtbl = longtbl.merge(imgtbls['abcd_midasemp101'].loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_mid_all_sem_dof']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)


longtbl = longtbl.merge(imgtbls['mrisst02'].loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_sst_all_beta_dof']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)
longtbl = longtbl.merge(imgtbls['mrisstsem01'].loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_sst_all_sem_dof']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)


longtbl = longtbl.merge(imgtbls['nback_bwroi02'].loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_nback_all_beta_dof']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)
longtbl = longtbl.merge(imgtbls['nbackallsem01'].loc[:, ['subjectkey', 'interview_date', 'eventname', 'tfmri_nback_all_sem_dof']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)

longtbl = longtbl.merge(mrfind.loc[:, ['subjectkey', 'interview_date', 'eventname', 'mrif_score', 'mrif_hydrocephalus', 'mrif_herniation']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)

bn_t, bn_l = load_abcd_table(release_dir / 'abcd_betnet02.txt')
# Network to subcortical table
ns_t, ns_l = load_abcd_table(release_dir /  'mrirscor02.txt')
# We'll merge on subjectkey and interview_date, filter out the other columns from ns_t before merging
ns_t.drop(ns_l.loc[ns_l.doc.isin(bn_l.doc),'name'].drop([3,5]).values,
          axis=1,
          inplace=True)

con = bn_t.merge(ns_t,
                 how='outer',
                 on=['subjectkey','interview_date'])

con_l = pd.concat([bn_l, 
                   ns_l[~ns_l.name.isin(bn_l.loc[bn_l.doc.isin(ns_l.doc),'name'].values)]]).reset_index(drop=True)
longtbl = longtbl.merge(con.loc[:, ['subjectkey', 'interview_date', 'eventname','rsfmri_cor_network.gordon_ntpoints']],
                how='left',
                on=['subjectkey','interview_date', 'eventname'],
                indicator=True)
assert longtbl.groupby('_merge')['collection_id'].count()['both'] == len(longtbl)
longtbl.drop('_merge', axis=1, inplace=True)

longtbl['mr_findings_ok'] = (longtbl['mrif_score'] < 3)  & (longtbl['mrif_hydrocephalus'] == 'no') &  (longtbl['mrif_herniation'] == 'no')

# Default everyting to qc_ok as False
longtbl['qc_ok'] = False

longtbl['bids_meta__subject_id'] = longtbl.subjectkey.str.replace('_','')

mriqc_long = mriqc_merge.merge(longtbl, how='left', on='bids_meta__subject_id', indicator='long_merge')
assert mriqc_long.shape[0] == mriqc_long.shape[0]

mriqc_long.groupby('long_merge').provenance__md5sum.count()

mriqc_long.bids_meta__modality.unique()

# QC qualifications for smri

t1_metrics = ['area', 'sulc', 't1w', 't1w.contrast', 't1w.gray02', 't1w.white02', 'thick', 'vol']
t2_metrics = ['t2w', 't2w.contrast', 't2w.gray02', 't2w.white02']


mriqc_long.loc[((mriqc_long.bids_meta__modality == 'T1w') 
              & (mriqc_long.fsqc_qc == 1)), 'qc_ok'] = True


mriqc_long.loc[((mriqc_long.bids_meta__modality == 'T2w') 
              & (mriqc_long.fsqc_qc == 1)), 'qc_ok'] = True

# QC for MID task
mriqc_long.loc[((mriqc_long.bids_meta__modality == 'bold') 
             & (mriqc_long.bids_meta__TaskName == 'mid')
             & (mriqc_long.fsqc_qc == 1)
             & (mriqc_long.iqc_mid_ok_ser > 0)
             & (mriqc_long['tfmri_mid_beh_perform.flag'] == 1)
             & (mriqc_long.tfmri_mid_all_beta_dof > 200)
             & (mriqc_long.tfmri_mid_all_sem_dof > 200)), 'qc_ok'] = True

# QC for SST
mriqc_long.loc[((mriqc_long.bids_meta__modality == 'bold') 
             & (mriqc_long.bids_meta__TaskName == 'sst')
             & (mriqc_long.fsqc_qc == 1)
             & (mriqc_long.iqc_sst_ok_ser > 0)
             & (mriqc_long['tfmri_sst_beh_perform.flag'] == 1)
             & (mriqc_long.tfmri_sst_all_beta_dof > 200)
             & (mriqc_long.tfmri_sst_all_sem_dof > 200)), 'qc_ok'] = True

# QC for nBack
mriqc_long.loc[((mriqc_long.bids_meta__modality == 'bold') 
             & (mriqc_long.bids_meta__TaskName == 'nback')
             & (mriqc_long.fsqc_qc == 1)
             & (mriqc_long.iqc_nback_ok_ser > 0)
             & (mriqc_long['tfmri_nback_beh_perform.flag'] == 1)
             & (mriqc_long.tfmri_nback_all_beta_dof > 200)
             & (mriqc_long.tfmri_nback_all_sem_dof > 200)), 'qc_ok'] = True

# QC for rest
mriqc_long.loc[((mriqc_long.bids_meta__modality == 'bold') 
             & (mriqc_long.bids_meta__TaskName == 'rest')
             & (mriqc_long['rsfmri_cor_network.gordon_ntpoints'] > 375)), 'qc_ok'] = True

mriqc_all = mriqc_merge.merge(mriqc_long.loc[:,['provenance__md5sum', 'qc_ok']], on='provenance__md5sum', indicator='all_merge')
assert mriqc_all.groupby('all_merge').provenance__md5sum.count()['right_only'] == 0
assert mriqc_all.groupby('all_merge').provenance__md5sum.count()['both'] != 0
assert mriqc_all.groupby('all_merge').provenance__md5sum.count()['left_only'] == 0

mriqc_all.drop(['_merge', 'all_merge'], axis=1)

mriqc_all.to_csv('/abcdqc_data/batchserver/output/df_plus_meta.csv', index=False)

iqms = [ 'cjv', 'cnr', 'efc', 'fber', 'fwhm_avg', 'fwhm_x', 'fwhm_y', 'fwhm_z', 'icvs_csf', 'icvs_gm', 'icvs_wm', 'inu_med', 'inu_range',
      'qi_1', 'qi_2', 'rpve_csf', 'rpve_gm', 'rpve_wm', 'size_x', 'size_y', 'size_z', 'snr_csf', 'snr_gm',
      'snr_total', 'snr_wm', 'snrd_csf', 'snrd_gm', 'snrd_total', 'snrd_wm', 'spacing_x', 'spacing_y', 'spacing_z', 'summary_bg_k',
      'summary_bg_mad', 'summary_bg_mean', 'summary_bg_median', 'summary_bg_n', 'summary_bg_p05', 'summary_bg_p95',
      'summary_bg_stdv', 'summary_csf_k', 'summary_csf_mad', 'summary_csf_mean', 'summary_csf_median', 'summary_csf_n',
      'summary_csf_p05', 'summary_csf_p95', 'summary_csf_stdv', 'summary_gm_k', 'summary_gm_mad', 'summary_gm_mean',
      'summary_gm_median', 'summary_gm_n', 'summary_gm_p05', 'summary_gm_p95', 'summary_gm_stdv', 'summary_wm_k',
      'summary_wm_mad', 'summary_wm_mean', 'summary_wm_median', 'summary_wm_n', 'summary_wm_p05', 'summary_wm_p95',
      'summary_wm_stdv', 'tpm_overlap_csf', 'tpm_overlap_gm', 'tpm_overlap_wm', 'wm2max' ]

t1_iqms = (mriqc_all.query('bids_meta__modality == "T1w"').describe().sum(0) != 0)
#t1_iqms.index[t1_iqms]
t2_iqms = (mriqc_all.query('bids_meta__modality == "T2w"').describe().sum(0) != 0)
#t2_iqms.index[t2_iqms]
bold_iqms = (mriqc_all.query('bids_meta__modality == "bold"').describe().sum(0) != 0)
bold_iqms.index[bold_iqms]

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
