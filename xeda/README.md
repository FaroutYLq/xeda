# Core Functions

## Scope
Core functions for `xeda`.

## Files
- `adv_analysis.py`: `.npy` output based advanced disk analysis. Results interpreted in `../results.ipynb`.
- `analysis.py`: `.txt` output based disk analysis. Human readable results are produced.
- `delete.py`: `admix` wrapper inheritted from data manager. 
- `dbclean_pymongo.py`: mimic what `admix` do when deleting data entry from RunDB.
- `dbclean_rundbapi.py`: use rundbapi deleting data entry from RunDB.
- 'delete_remaining_rucio_data.py': clean up disk by brutal force. **Only use this after cleaning rucio+DB!!**
- `find_rules`: core for `rucio` rules scanning. 
- `tar_and_split.py`: tar and split a folder for archiving. Check `../jobs` folder for usage.
- `xeda.py`: core for disk usage scanning. Check `../README.md` and `../jobs` for usage.
- `utilities.py`: useful functions for analyzing rucio scan results.

## Usage
It is not recommended to run them individually. You can find more information on usage in the `README.md` of the parent folder, and in `jobs` folder. 

If there is a script you don't find any usage tutorial anywhere, don't use it.