import numpy as np
import time
import os, shlex
import sys
import utilix
from utilix.batchq import *

print(utilix.__file__)

TO_SCAN = ["/project/lgrandi/", "/project2/lgrandi/", "/dali/lgrandi/"]
QOS = {"/dali/lgrandi/": "dali", "/project2/lgrandi/": "xenon1t", "/project/lgrandi/": "xenon1t"}
PARTITION = {"/dali/lgrandi/": "dali", "/project2/lgrandi/": "xenon1t", "/project/lgrandi/": "xenon1t"}

class Submit(object):
    """
    Take maximum number of nodes to use at once
    Submit each group to a node and excute
    """

    def name(self):
        return self.__class__.__name__

    def execute(self, *args, **kwargs):
        eval("self.{name}(*args, **kwargs)".format(name=self.name().lower()))

    def submit(self, loop_over=[], max_num_submit=10, nmax=3):
        _start = 0
        self.max_num_submit = max_num_submit
        self.loop_over = loop_over
        self.p = True

        index = _start
        while index < len(self.loop_over) and index < nmax:
            if self.working_job() < self.max_num_submit:
                self._submit_single(loop_index=index, loop_item=self.loop_over[index])

                time.sleep(1.0)
                index += 1

    # check my jobs
    def working_job(self):
        cmd = "squeue --user={user} | wc -l".format(user="yuanlq")
        jobNum = int(os.popen(cmd).read())
        return jobNum - 1

    def _submit_single(self, loop_index, loop_item):
        jobname = "scan_%s" % (loop_item.split("/")[1])
        scan_within = loop_item
        # Modify here for the script to run
        jobstring = (
            "python /home/yuanlq/software/xeda/xeda.py -t 500GB -o '/project2/lgrandi/yuanlq/shared/disk_usage/' -d '[xenonnt, xenon1t]' -s %s"
            % (loop_item)
        )
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring,
            log="/home/yuanlq/.tmp_job_submission/scan_%s.log"
            % (loop_item.split("/")[1]),
            partition=PARTITION[loop_item],
            qos=QOS[loop_item],
            account="pi-lgrandi",
            jobname=jobname,
            delete_file=True,
            dry_run=False,
            mem_per_cpu=25000,
            container="xenonnt-development.simg",
            cpus_per_task=1,
        )


p = Submit()
loop_over = TO_SCAN
p.execute(loop_over=loop_over, max_num_submit=5, nmax=10000)
