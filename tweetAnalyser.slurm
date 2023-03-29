#!/bin/bash

module load foss/2019b
module load python/3.7.4
module load mpi4py/3.0.2-timed-pingpong
module load numpy/1.18.0-python-3.7.4

n_cores=$1

mpirun -np ${n_cores} python3 tweetAnalyser.py -s sal.json -d bigTwitter.json

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s
