#!/bin/bash
#SBATCH --partition=interactive
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-00:05:00
#SBATCH --output=2n8c.out
#SBATCH --error=2n8c.err

# Load required modules

module load gcccore/8.3.0
module load foss/2019b
module load python/3.7.4
module load numpy/1.18.0-python-3.7.4


time srun -n 8 python3 ass1.py
echo "Completed!...................................."
