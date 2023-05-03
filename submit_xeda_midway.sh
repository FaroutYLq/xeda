module load singularity
singularity shell --bind /project2 --bind /project /project2/lgrandi/xenonnt/singularity-images/xenonnt-development.simg
python /home/yuanlq/software/xeda/batch_job.py midway
exit
