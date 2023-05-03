module load singularity
singularity shell --bind /project2 --bind /project --bind /scratch/midway2/$USER --bind /scratch/midway3/$USER /project2/lgrandi/xenonnt/singularity-images/xenonnt-development.simg
python /home/yuanlq/software/xeda/batch_job.py midway
exit
