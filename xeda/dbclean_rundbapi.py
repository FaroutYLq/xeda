# This script in principle can work on ap23

from utilix import DB, uconfig
import numpy as np
from tqdm import tqdm
import time
import os
import sys

_, did_npy_path = sys.argv
dids_to_delete = np.load(did_npy_path, allow_pickle=True)

os.environ['XENON_CONFIG']='/home/yuanlq/.xenon_config'
os.environ['RUCIO_ACCOUNT']='production'
os.environ['X509_USER_PROXY']='/home/yuanlq/user_cert'

runsDB_API = DB()

def remove_data_entries(runsDB_API, dids_to_delete, dry=False):
    bad_attempts = []
    
    if len(dids_to_delete.dtype) == 0:
        all_sites = True
    else:
        all_sites = False
    
    print("Want to delete %s data entries"%(len(dids_to_delete)))
    for rule_info in tqdm(dids_to_delete):
        if not all_sites:
            did = rule_info['dataset_did']
            rse = rule_info['rse']
        else:
            did = rule_info
        
        runid = did.split(':')[0].split('_')[-1]
        all_data = runsDB_API.get_data(runid)

        found_entry = False
        for d in all_data:
            if ('location' in d.keys()) and ('did' in d.keys()):
                if all_sites:
                    if d['did'] == did:
                        d_to_delete = d
                        found_entry = True
                        break
                else:
                    if d['did'] == did and d['location'] == rse:
                        d_to_delete = d
                        found_entry = True
                        break
        if not found_entry:
            print('Cannot find did %s in document!'%(did))
            print('Skipping...')
            bad_attempts.append(did)

        if found_entry:
            if dry:
                print("Want to delete %s's entry as below"%(runid))
                print(d_to_delete)
            else:
                # This is EXTREMELLY DANGEROUS!!!
                try:
                    print(d_to_delete)
                    runsDB_API.delete_data(runid, d_to_delete)
                    time.sleep(0.5)
                except:
                    try:
                        print("Failed deletion using 6-fig runid, trying 5-fig runid...")
                        runsDB_API.delete_data(str(int(runid)), d_to_delete)
                    except:
                        print('Failed again to delete did %s in document!'%(did))
                        print('Skipping...')
                        print('Error message:')
                        print(sys.exc_info()[0])
                        bad_attempts.append(did)
                    

    print('-------')
    print('SUMMARY')
    if len(bad_attempts) == 0:
        print('All deletion attempts succeeded for %s data entries!'%(len(dids_to_delete)))
    else:
        print('%s/%s deletion attempts succeeded...'%(len(dids_to_delete)-len(bad_attempts),len(dids_to_delete)))
        print('There are %s failures listed below'%(len(bad_attempts)))
        print(bad_attempts)
    print('-------')
        
        
if __name__ == '__main__':
    remove_data_entries(runsDB_API, 
                        dids_to_delete, 
                        dry=False)
