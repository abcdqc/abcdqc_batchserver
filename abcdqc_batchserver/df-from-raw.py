import pandas as pd
import os
import re
import json
import time
import unittest

jsonList = []
for root, dirs, files in os.walk("/abcdqc_data/mriqc/"):
    for filename in files:
        if re.match(".*json$", filename) is not None:
            jsonList.append(root + '/' + filename)

# https://stackoverflow.com/questions/52782024/reading-dictionary-stored-on-text-file-and-convert-to-pandas-dataframe
def readjson(file):
    with open(file) as f:
        jsonstr = json.load(f)

    return(pd.io.json.json_normalize(jsonstr))

count = 1
json_list = []
start_time = time.time()
concat_at = 1000
df = pd.DataFrame()
for ji, file in enumerate(jsonList):
    if ji % 1000 == 0:
        print(str(ji) + "/" + str(len(jsonList)) + "," + str(time.time() - start_time) + " seconds", flush=True)
        if json_list != []:
            df = df.append(pd.concat(json_list, sort=True, ignore_index=True, copy=False), ignore_index=True)
            json_list=[]
    json_list.append(readjson(file))
df = df.append(pd.concat(json_list, sort=True, ignore_index=True, copy=False), ignore_index=True)

df.to_csv('/abcdqc_data/batchserver/output/df.csv', index=False)

## Testing Framework

# readjson
def readjson_test():
    dat = readjson('/abcdqc_data/batchserver/test/input/test.json')
    print(dat.columns.values)
    col_t = ['bids_meta.AcquisitionMatrixPE', 'bids_meta.AcquisitionNumber', 'bids_meta.AcquisitionTime', 'bids_meta.ConversionSoftware', 'bids_meta.ConversionSoftwareVersion', 'bids_meta.DeviceSerialNumber', 'bids_meta.EchoTime', 'bids_meta.FlipAngle',
 'bids_meta.ImageOrientationPatientDICOM', 'bids_meta.ImageType',
 'bids_meta.InPlanePhaseEncodingDirectionDICOM', 'bids_meta.InversionTime',
 'bids_meta.MRAcquisitionType', 'bids_meta.MagneticFieldStrength',
 'bids_meta.Manufacturer', 'bids_meta.ManufacturersModelName',
 'bids_meta.Modality', 'bids_meta.PatientPosition',
 'bids_meta.PercentPhaseFOV', 'bids_meta.PixelBandwidth',
 'bids_meta.ProtocolName', 'bids_meta.ReconMatrixPE',
 'bids_meta.RepetitionTime', 'bids_meta.SAR', 'bids_meta.ScanOptions',
 'bids_meta.ScanningSequence', 'bids_meta.SequenceVariant',
 'bids_meta.SeriesDescription', 'bids_meta.SeriesNumber',
 'bids_meta.SliceThickness', 'bids_meta.SoftwareVersions',
 'bids_meta.SpacingBetweenSlices', 'bids_meta.dataset', 'bids_meta.modality',
 'bids_meta.session_id', 'bids_meta.subject_id', 'cjv', 'cnr', 'efc', 'fber',
 'fwhm_avg', 'fwhm_x', 'fwhm_y', 'fwhm_z', 'icvs_csf', 'icvs_gm', 'icvs_wm',
 'inu_med', 'inu_range', 'provenance.md5sum', 'provenance.settings.testing',
 'provenance.software', 'provenance.version',
 'provenance.warnings.large_rot_frame',
 'provenance.warnings.small_air_mask', 'provenance.webapi_port',
 'provenance.webapi_url', 'qi_1' 'qi_2', 'rpve_csf', 'rpve_gm', 'rpve_wm',
 'size_x', 'size_y', 'size_z', 'snr_csf', 'snr_gm', 'snr_total', 'snr_wm',
 'snrd_csf', 'snrd_gm', 'snrd_total', 'snrd_wm', 'spacing_x', 'spacing_y',
 'spacing_z', 'summary_bg_k', 'summary_bg_mad', 'summary_bg_mean',
 'summary_bg_median', 'summary_bg_n', 'summary_bg_p05', 'summary_bg_p95',
 'summary_bg_stdv', 'summary_csf_k', 'summary_csf_mad', 'summary_csf_mean',
 'summary_csf_median', 'summary_csf_n', 'summary_csf_p05', 'summary_csf_p95',
 'summary_csf_stdv', 'summary_gm_k', 'summary_gm_mad', 'summary_gm_mean',
 'summary_gm_median', 'summary_gm_n', 'summary_gm_p05', 'summary_gm_p95',
 'summary_gm_stdv', 'summary_wm_k', 'summary_wm_mad', 'summary_wm_mean',
 'summary_wm_median', 'summary_wm_n', 'summary_wm_p05', 'summary_wm_p95',
 'summary_wm_stdv', 'tpm_overlap_csf', 'tpm_overlap_gm', 'tpm_overlap_wm',
 'wm2max']
    print(col_t)
    #assert dat.columns.values == col_t

# uncomment below to run tests
readjson_test()
