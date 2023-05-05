# MPI-Analysis-of-Covid19-Data
MPI project computing a 7-day moving average and the percentage increase of the moving average of the number of cases, in order to track everyday how every Country is doing. <br><br>
The dataset we used is from https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide <br>

Need to have openmpi installed on your machine.

## Compile and run the program
mpicc main.c -o main <br>
mpirun -np N main <br>
N is the number of processes you want to allow the program to run on. <br><br>
To run it on multiple hosts, they have to be connected via passwordless ssh and have the repo installed in the same place in the file system. It's also required that the OS be the same and the binaries compiled with the same compiler. After compiling on each machine, run (having N=M+K): <br>
mpirun -np N --host hostname1:M,hostname2:K main <br>

## Assumptions
- Need at least N = 2
- The dataset is the complete one from the website (already downloaded in the repo)

## Contributors
- Francesco Scandale
- Luca Rondini
- Marco Zanghieri
