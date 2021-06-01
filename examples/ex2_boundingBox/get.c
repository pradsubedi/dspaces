/* get.c : Example 3: DataSpaces get Bounding Box tutorial
 *  */
#include "dspaces.h"
#include "mpi.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

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

    // DataSpaces: Initalize and identify application
    // Usage: dspaces_init(num_peers, appid, Ptr to MPI comm, parameters)
    // Note: appid for get.c is 2 [for put.c, it was 1]
    dspaces_init_mpi(gcomm, &client);

    int timestep = 0;

    while(timestep < 10) {
        timestep++;

        // Name our data.
        char var_name[128];
        sprintf(var_name, "ex3_sample_data");

        // We plan to access 10 values from two boxes
        int *data = malloc(10 * sizeof(int));

        // Define the dimensionality of the data to be received
        int ndim = 1;

        // Prepare LOWER and UPPER bound dimensions
        uint64_t lb, ub;

        // In put example, we had 10 boxes. Let's acess values in
        // boxes 3,0,0 and 4,0,0 at each timestep
        lb = 3;
        ub = 12;

        // DataSpaces: Get data array from the space
        // Usage: dspaces_get(Name of variable, version num,
        // size (in bytes of each element), dimensions for bounding box,
        // lower bound coordinates, upper bound coordinates,
        // ptr to data buffer
        dspaces_get(client, var_name, timestep, sizeof(int), ndim, &lb, &ub,
                    data, -1);

        printf("Timestep %d: get data in Bounding Box: LB: %" PRIu64
               " UB: %" PRIu64 "\n",
               timestep, lb, ub);

        printf("Data:\n");
        int i;
        for(i = 0; i < 10; i++) {
            printf("%d\t", data[i]);
        }
        printf("\n");

        free(data);
    }

    dspaces_kill(client);

    // DataSpaces: Finalize and clean up DS process
    dspaces_fini(client);

    MPI_Barrier(gcomm);
    MPI_Finalize();

    return 0;
}
