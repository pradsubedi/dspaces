/* put.c : Example 3: DataSpaces Bounding Box Tutorial
 */
#include "dspaces.h"
#include "mpi.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

// compare function: used to quicksort array values
int compare(const void *a, const void *b) { return (*(int *)a - *(int *)b); }

int main(int argc, char **argv)
{
    int err;
    int nprocs, rank;
    MPI_Comm gcomm;
    dspaces_client_t client;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Barrier(MPI_COMM_WORLD);
    gcomm = MPI_COMM_WORLD;

    // Initalize DataSpaces
    // # of Peers, Application ID, ptr MPI comm, additional parameters
    // # Peers: Number of connecting clients to the DS server
    // Application ID: Unique idenitifier (integer) for application
    // Pointer to the MPI Communicator, allows DS Layer to use MPI barrier func
    // Addt'l parameters: Placeholder for future arguments, currently NULL.
    dspaces_init_mpi(gcomm, &client);

    int timestep = 0;

    while(timestep < 10) {
        timestep++;
        sleep(2);

        // Name the Data that will be writen
        char var_name[128];
        sprintf(var_name, "ex3_sample_data");

        // Initialize Random Number Generator
        srand(time(NULL));

        // Create integer array, size 50
        int *data = malloc(50 * sizeof(int));

        // Generate 50 random numbers between 0 and 128
        // for each timestep -- these numbers should be different
        int j;
        for(j = 0; j < 50; j++) {
            data[j] = rand() % 128;
        }

        // Sort array
        qsort(data, 50, sizeof(int), compare);

        // We will use 1D
        int ndim = 1;

        uint64_t lb = 0, ub = 49;

        dspaces_put(client, var_name, timestep, sizeof(int), ndim, &lb, &ub,
                    data);

        printf("Finished put 50 values.\n");

        free(data);
    }

    dspaces_kill(client);

    // DataSpaces: Finalize and clean up DS process
    dspaces_fini(client);

    MPI_Barrier(gcomm);
    MPI_Finalize();

    return 0;
}
