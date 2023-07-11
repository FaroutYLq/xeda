import numpy as np
import sys
import pymongo
from tqdm import tqdm
from utilix import xent_collection
from utilix import DB
import numpy as np
import admix
from datetime import datetime


_, job_i, n_runs, output_path = sys.argv
n_runs = int(eval(n_runs))
print("Loaded the arguments successfully, and the number of runs to process:", n_runs)


def list_dataset_dids(to_delete_runids, raw_type_only=False, peak_type_only=False, 
                      exclude_peaklets_list=[]):
    """Given the runids of datasets, find the corresponding datasets in rucio database.
    parameter
    ---------
    to_delete_runids[list]: runids to delete some data eg. '007330'
    raw_type_only[bool]: Will we only include the admix rawtype for the output datasets?
    
    return
    ------
    dataset_dids[list]: Dids of datasets to delete
    """
    print('Searching for DIDs for the runs to ')        
    dataset_dids = []
    for runid in tqdm(to_delete_runids):
        if type(runid) == int or type(runid) == np.int64:
            runid = str(runid).zfill(6)
        # print('Searching for DIDs for run %s'%(runid))
        # Convert runids into rucio scopes
        scope = 'xnt_' + runid
        # Get names of datasets for certain runids from rucio database
        names = admix.rucio.list_datasets(scope)
        
        # Find DIDs for the datasets to delete
        if raw_type_only: # If we only want to delete rawdtypes.
            for name in names:
                if name.split('-')[0] in admix.utils.RAW_DTYPES:
                    dataset_dids.append(scope+':'+name)
                else:
                    pass
        elif peak_type_only:
            for name in names:
                if name.split('-')[0] in ['peaklets', 'merged_s2s', 'hitlets_nv']:
                    if name.split('-')[0] == 'peaklets' and scope.split('_')[1] in exclude_peaklets_list:
                        pass
                    else:
                        dataset_dids.append(scope+':'+name)
                else:
                    pass
        else: # If we want to delete all dtypes
            for name in names:
                dataset_dids.append(scope+':'+name)
    
    print('Finished search for dataset DIDs.')
    return dataset_dids
    

def find_rules_info(dataset_dids):
    """Find the number of rules, rse, and size for each dataset.
    parameter
    --------
    dataset_dids[list]: Dids of datasets to delete.
    
    return
    ------
    rules_info[ndarray]: number of rules, corresponding rses, size in GB
    """
    dtype = [
        (('did of a dataset', 'dataset_did'), 'O'),
        (('number of rules','n_rules'),np.int8),
        (('size of a rule in GB','size_gb'),np.float32),
        (('Stored in rse CCIN2P3_USERDISK', 'CCIN2P3_USERDISK'), np.bool),
        (('Stored in rse CNAF_TAPE2_USERDISK', 'CNAF_TAPE2_USERDISK'), np.bool),
        (('Stored in rse CNAF_TAPE3_USERDISK', 'CNAF_TAPE3_USERDISK'), np.bool),
        (('Stored in rse CNAF_TAPE_USERDISK', 'CNAF_TAPE_USERDISK'), np.bool),
        (('Stored in rse CNAF_USERDISK', 'CNAF_USERDISK'), np.bool),
        (('Stored in rse LNGS_USERDISK', 'LNGS_USERDISK'), np.bool),
        (('Stored in rse NIKHEF2_USERDISK', 'NIKHEF2_USERDISK'), np.bool),
        (('Stored in rse NIKHEF_USERDISK', 'NIKHEF_USERDISK'), np.bool),
        (('Stored in rse SDSC_USERDISK', 'SDSC_USERDISK'), np.bool),
        (('Stored in rse SDSC_NSDF_USERDISK', 'SDSC_NSDF_USERDISK'), np.bool),
        (('Stored in rse SURFSARA_USERDISK', 'SURFSARA_USERDISK'), np.bool),
        (('Stored in rse UC_DALI_USERDISK', 'UC_DALI_USERDISK'), np.bool),
        (('Stored in rse UC_MIDWAY_USERDISK', 'UC_MIDWAY_USERDISK'), np.bool),
        (('Stored in rse UC_OSG_USERDISK', 'UC_OSG_USERDISK'), np.bool),
        (('Lineage hash', 'hash'), 'O'),
        (('Data type', 'data_type'), 'O'),
        (('RunID', 'runid'), 'O'),
        ]
    rules_info = np.zeros(len(dataset_dids), dtype=dtype)
    
    for i in tqdm(range(len(dataset_dids))):
        did = dataset_dids[i]
        rules_info[i]['dataset_did'] = did
        rules = admix.rucio.list_rules(did)
        # number of rules
        rules_info[i]['n_rules'] = len(rules)
        # size
        rules_info[i]['size_gb'] = admix.rucio.get_size_mb(did)/1024
        
        # Add rse information
        for rule in rules:
            try:
                rules_info[i][rule['rse_expression']] = True
            except:
                pass
        rules_info[i]['hash'] = did.split('-')[1]
        rules_info[i]['data_type'] = did.split(':')[1].split('-')[0]
        rules_info[i]['runid'] = did.split(':')[0].split('_')[1]
    
    return rules_info

dataset_dids = list_dataset_dids(to_delete_runids=np.arange(n_runs)+int(n_runs*eval(job_i)), 
                                 raw_type_only=False, peak_type_only=False,
                                 exclude_peaklets_list=[])
rules_info = find_rules_info(dataset_dids)
np.save('%srucio_%s_all_rules%s'%(output_path, 
                                  datetime.now().strftime('%Y%m%d'), 
                                  job_i),
        rules_info)
print('Done!')
