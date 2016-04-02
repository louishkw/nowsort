/*
 * Authors: Wolf Honore, Victor Liu, Ka Wo Hong
 * Assignment: CSC 258 Project (Spring 2016)
 *
 * Description:
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int nprocs;
int rank;

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Finalize();
    return 0;
}
