# This script in principle can work on ap23

from utilix import DB, uconfig
import numpy as np
from tqdm import tqdm
import time
import os
import sys

_, did_npy_path, rse = sys.argv
dids_to_delete = np.load(did_npy_path)

os.environ['XENON_CONFIG']='/home/yuanlq/.xenon_config'
os.environ['RUCIO_ACCOUNT']='production'
os.environ['X509_USER_PROXY']='/home/yuanlq/user_cert'

runsDB_API = DB()

def remove_data_entries(runsDB_API, dids_to_delete, rse, dry=False):
    bad_attempts = []
    
    print("Want to delete %s data entries"%(len(dids_to_delete)))
    for did in tqdm(dids_to_delete):
        runid = did.split(':')[0].split('_')[-1]
        all_data = runsDB_API.get_data(runid)

        found_entry = False
        for d in all_data:
            if ('location' in d.keys()) and ('did' in d.keys()):
                if d['location'] == rse and d['did'] == did:
                    d_to_delete = d
                    #print('Found did %s at %s in document!'%(did, rse))
                    found_entry = True
                    break
        if not found_entry:
            print('Cannot find did %s at %s in document!'%(did, rse))
            print('Skipping...')
            bad_attempts.append(did)

        if found_entry:
            if dry:
                print("Want to delete %s's entry as below"%(runid))
                print(d_to_delete)
            else:
                # This is EXTREMELLY DANGEROUS!!!
                runsDB_API.delete_data(runid, d_to_delete)
                time.sleep(2)
                

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
                        rse=rse, 
                        dry=True)