#!/bin/bash

#SBATCH --time=00:05:00
#SBATCH --output=/home/yuanlq/.tmp_job_submission/cron_xeda.log
#SBATCH --open-mode=append
#SBATCH --account=cron-account
#SBATCH --partition=cron
#SBATCH --qos=cron

# Specify a valid Cron string for the schedule. This specifies that
# the Cron job run once per day at 3:15am.
SCHEDULE='15 3 * * *'

# Load environment and submit scan jobs.
. submit_xeda_dali.sh
. submit_xeda_midway2.sh
. submit_xeda_midway3.sh

# This schedules the next run.
sbatch --quiet --begin=$(date -d '+24 hour' '+%FT%T') cron_xeda.sbatch

