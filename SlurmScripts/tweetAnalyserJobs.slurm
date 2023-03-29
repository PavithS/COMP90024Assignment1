#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:01:00
#SBATCH --output=tweetAnalyserJobs.out

sbatch --nodes=1 --ntasks-per-node=1 --time=00:10:00 --job-name=tweetAnalyser1n1c --output=tweetAnalyser1n1c.out tweetAnalyser.slurm 1
sbatch --nodes=1 --ntasks-per-node=8 --time=00:02:00 --job-name=tweetAnalyser1n8c --output=tweetAnalyser1n8c.out tweetAnalyser.slurm 8
sbatch --nodes=2 --ntasks-per-node=4 --time=00:02:00 --job-name=tweetAnalyser2n4c --output=tweetAnalyser2n4c.out tweetAnalyser.slurm 8


##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list resource usage
my-job-stats -a -n -s
