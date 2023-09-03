from utilix.rundb import xent_collection
import numpy as np
from tqdm import tqdm
import sys
import datetime

_, did_npy_path, rse  = sys.argv

dids_to_remove = np.load(did_npy_path)
print("datasets to remove from DB: ",len(dids_to_remove))


def RemoveDatafield(db, id_field, rem_dict):
    """
    Remove a data field from a run document in the database.
    WARNING: we are pretending we are admix!!
    """
    new_entry = dict(rem_dict.items())
    new_entry.update({"at" : datetime.datetime.utcnow(), "by" : "admix"})
    
    db.update_one({"_id" : id_field},
                              {"$pull" : {"data" : rem_dict},
                               "$push" : {"deleted_data" : new_entry}})

db = xent_collection()

for did in tqdm(dids_to_remove):
    lineage_hash = did.split('-')[-1]
    dtype = did.split('-')[0].split(':')[-1]
    number = int(did.split(':')[0].split('_')[-1])

    print("  Deleting the db entry {0} of the rse {1}".format(did,rse))
    print("  Run number: {0}".format(number))
    print("  Data type: {0}".format(dtype))
    print("  Hash: {0}".format(lineage_hash))
    run = db.find_one({'number' : number})

    #Checks if the datum exists in the DB
    datum = None
    for d in run['data']:
        if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == rse:
            datum = d
            break

    #Delete the datum
    if datum is not None:
        RemoveDatafield(db, run['_id'],datum)
        print(datum)
        print("Datum deleted in DB.")
    else:
        print('There is no datum to delete')

