import numpy as np
import time
import os, shlex
import sys
import utilix
from utilix.batchq import *

print(utilix.__file__)

QOS = {"dali": "dali", "project2": "xenon1t", "project": "xenon1t"}
PARTITION = {"dali": "dali", "project2": "xenon1t", "project": "xenon1t"}

_, absolute_dir = sys.argv
assert absolute_dir[0] == "/", "Please input an absolute directory!"
parent_dir = absolute_dir.split("/")[1]
assert parent_dir in (
    "dali",
    "project2",
    "project",
), "Now this script only supports dali or project2."


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
        jobname = "tar_%s" % (loop_item)
        # Modify here for the script to run
        jobstring = "python /home/yuanlq/software/xeda/tar_and_split.py %s" % (
            loop_item
        )
        print("Running this job string: ")
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring,
            log="/home/yuanlq/.tmp_job_submission/tar_%s_%s.log"
            % (loop_item.split("/")[1], loop_item.split("/")[3]),
            partition=PARTITION[parent_dir],
            qos=QOS[parent_dir],
            account="pi-lgrandi",
            jobname=jobname,
            delete_file=True,
            dry_run=False,
            mem_per_cpu=4000,
            container="xenonnt-development.simg",
            cpus_per_task=1,
        )


p = Submit()
loop_over = [absolute_dir]
p.execute(loop_over=loop_over, max_num_submit=5, nmax=10000)
