module load singularity
singularity shell --bind /dali /dali/lgrandi/xenonnt/singularity-images/xenonnt-development.simg
python /home/yuanlq/software/xeda/batch_job.py dali
exit
