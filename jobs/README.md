# Job Submission
## Scope
Job submission wrappers for `xeda`.

## Files
- `cron_xeda.sbatch`: Cron job submission sbatch. It will in principle scan the disk everyday.
- `batch_job.py`: Batch job submitter for disk scanning.
- `batch_job_tar.py`: Batch job submitter for file archiving.
- `batch_job_rules.py`: Batch job submitter for rucio rule scanning.
- `batch_job_reprocessed_peaks.py`: Batch job submitter for `peaks` loading test, in case of data corruption.
- `build_peaks.py`: Job file for `peaks` loading test.
- `submit_xeda_dali.sh`: Submit disk scanning job for `dali`.
- `submit_xeda_midway2.sh`: Submit disk scanning job for `midway2`.
- `submit_xeda_midway3.sh`: Submit disk scanning job for `midway3`.

## Usage
- To tar and split a folder for archiving, run his
```
# Change the argument to be the desired folder to archive
python batch_job_tar.py /dali/lgrandi/yuanlq/
```
- To scan a certain server, run this
```
# Change the argument to be one of the following : dali, midway2, midway3
python batch_job.py midway2
```
- To start the cron job for disk scanning, run this on midway2/dali login nodes
```
sbatch cron_xeda.sbatch
```
- To start the scanning of rucio rule scanning:
```
python batch_job_rules.py
```
