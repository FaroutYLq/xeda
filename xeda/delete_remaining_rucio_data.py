# this script is to manually delete the trash rucio leftover after deleting rules
import numpy as np
import subprocess
import sys

_, did_npy_path, rucio_dir = sys.argv
assert rucio_dir[-1] == '/'
dids = np.load(did_npy_path)
failed = []

for did in dids:
    print('-----')
    print(did)
    scope, name = did.split(':')
    find_result = subprocess.run(['find', rucio_dir+scope, 
                                  '-type', 'f', '-name', name+'-*'], 
                                 capture_output=True, text=True)
    # if find something to delete
    if len(find_result.stdout):
        files = find_result.stdout.split('\n')[:-1]
        print('Need to delete %s files'%(len(files)))
        met_trouble = False
        # delete remaining files one by one
        for file in files:
            delete_result = subprocess.run(['rm', '-rf', file], 
                                           capture_output=True, text=True)
            # met troubel in deletion
            if len(delete_result.stderr):
                print(delete_result.stderr)
                met_trouble = True
        # register failure
        if met_trouble:
            failed.append(did)
        else:
            print("Deletion finished for %s files!"%(len(files)))
    else:
        if len(find_result.stderr):
            print(find_result.stderr)
        else:
            print('%s has nothing left on disk! Skipping...'%(did))

print('The following dids faield:')
print(failed)
