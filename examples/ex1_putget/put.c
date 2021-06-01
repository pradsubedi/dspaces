/* put.c : Example 1: DataSpaces put tutorial
 * This example will show you the simplest way
 * to put a 1D array of 3 elements into the DataSpace.
 * */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "dspaces.h"

int main(int argc, char **argv)
{
    dspaces_client_t client;

    // Initalize DataSpaces
    // # of Peers, Application ID, ptr MPI comm, additional parameters
    // # Peers: Number of connecting clients to the DS server
    // Application ID: Unique idenitifier (integer) for application
    // Pointer to the MPI Communicator:
    //      when NOT NULL, allows DS Layer to use MPI barrier func
    // Addt'l parameters: Placeholder for future arguments, currently NULL.
    dspaces_init(0, &client);

    int timestep = 0;

    while(timestep < 10) {
        timestep++;
        sleep(2);

        // Name the Data that will be writen
        char var_name[128];
        sprintf(var_name, "ex1_sample_data");

        // Create integer array, size 3
        int *data = malloc(3 * sizeof(int));

        // Initialize Random Number Generator
        srand(time(NULL));

        // Populate data array with random values from 0 to 99
        data[0] = rand() % 100;
        data[1] = rand() % 100;
        data[2] = rand() % 100;

        printf("Timestep %d: put data %d %d %d\n", timestep, data[0], data[1],
               data[2]);

        // ndim: Dimensions for application data domain
        // In this case, our data array is 1 dimensional
        int ndim = 1;

        // Prepare LOWER and UPPER bound dimensions
        // In this example, we will put all data into a
        // small array at the origin upper bound = lower bound = (0,0,0)
        // In further examples, we will expand this concept.
        uint64_t lb, ub;
        lb = 0;
        ub = 2;

        // DataSpaces: Put data array into the space
        // Usage: dspaces_put(Name of variable, version num,
        // size (in bytes of each element), dimensions for bounding box,
        // lower bound coordinates, upper bound coordinates,
        // ptr to data buffer
        dspaces_put(client, var_name, timestep, sizeof(int), ndim, &lb, &ub,
                    data);

        free(data);
    }

    // Signal the server to shutdown (the server must receive this signal n
    // times before it shuts down, where n is num_apps in dataspaces.conf)
    dspaces_kill(client);

    // DataSpaces: Finalize and clean up DS process
    dspaces_fini(client);

    return 0;
}
