module load singularity
singularity shell --bind /home/yuanlq --bind /project2 --bind /project/ --bind /scratch/midway2/yuanlq --bind /scratch/midway3/yuanlq /project2/lgrandi/xenonnt/singularity-images/xenonnt-development.simg
python /home/yuanlq/software/xeda/batch_job.py midway3
