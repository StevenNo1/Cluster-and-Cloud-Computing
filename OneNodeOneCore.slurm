#!/bin/bash
#SBATCH --partition=interactive
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0-00:10:00
#SBATCH --output=1n1c.out
#SBATCH --error=1n1c.err

# Load required modules

module load gcccore/8.3.0
module load foss/2019b
module load python/3.7.4
module load numpy/1.18.0-python-3.7.4


time srun -n 1 python3 ass1.py
echo "Completed!...................................."
