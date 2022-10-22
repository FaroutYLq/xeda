import sys
import os

# may look like '/dali/lgrandi/yuanlq/fool/'
_, absolute_dir = sys.argv
if absolute_dir[-1] != "/":
    absolute_dir += "/"

print("Now trying to tar this absolute directory: ", absolute_dir)

# look like '/dali/lgrandi/yuanlq'
usrname = absolute_dir.split("/")[3]
disk = absolute_dir.split("/")[1]
piname = absolute_dir.split("/")[2]

# look like '/dali/lgrandi/yuanlq_relocated/yuanlq'
os.system("mkdir /%s/%s/%s_relocated/" % (disk, piname, usrname))
temp_tar_dir = "/%s/%s/%s_relocated/" % (disk, piname, usrname) + usrname
# look like '/dali/lgrandi/yuanlq_relocated/yuanlq.tar.gz'
temp_tar_dir += ".tar.gz"

cmd_tar = "tar -czf %s %s" % (temp_tar_dir, absolute_dir)
print("Command for tar: ", cmd_tar)
# Hardcoded to split at 100G each now...
cmd_split = 'split -b 100G %s "%s"' % (temp_tar_dir, temp_tar_dir + ".part")
print("Command for split: ", cmd_split)

os.system(cmd_tar)
print("Finished tarring. Now splitting them into 100G chunks.")
os.system(cmd_split)

print("Already tarred and split. Now we delete the .tar.gz file!")
os.system("rm %s" % (temp_tar_dir))
print("Delted .tar.gz file!")

print(" ")
print("=============")
print("==Job done!==")
print("=============")
