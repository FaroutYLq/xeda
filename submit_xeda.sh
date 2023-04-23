module load singularity
singularity shell --bind /project2 --bind /project/lgrandi/ --bind /scratch/midway2/$USER --bind /project2/lgrandi/xenonnt/dali:/dali /project2/lgrandi/xenonnt/singularity-images/xenonnt-development.simg
python /home/yuanlq/software/xeda/batch_job.py
exit
