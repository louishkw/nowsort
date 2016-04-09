/*
 * Authors: Wolf Honore, Victor Liu, Ka Wo Hong
 * Assignment: CSC 258 Project (Spring 2016)
 *
 * Description:
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include "hdfs.h"

int nprocs;
int rank;

/*
 * Demonstrates how to read from hdfs.
 */
void readExample() {
    const char *fname = "test.dat";
    int nrecords = 1000;
    int record_size = 100;

    hdfsFS fs = hdfsConnect("default", 0);
    hdfsFile data_f = hdfsOpenFile(fs, fname, O_RDONLY, 0, 0, 0);

    char data[nrecords * record_size];
    hdfsRead(fs, data_f, data, nrecords * record_size);

    int i;
    for (i = 0; i < 7; i++)
        printf("%c", data[i]);
    printf("\n");

    hdfsCloseFile(fs, data_f);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    readExample();
    
    MPI_Finalize();
    return 0;
}
