#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=00:05:00
module load foss/2019b
module load python/3.7.4
module load mpi4py/3.0.2-timed-pingpong
module load numpy/1.18.0-python-3.7.4

mpirun -np 8 python3 tweetAnalyserMPI21.py -s sal.json -d bigTwitter.json

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s 
