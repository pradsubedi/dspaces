/* minmax_reader.c : Example 2: Min/Max/Average of Array using DataSpace
 * In this example, we will use a number of processes (specified by -np)
 * to compute the minimum and maximum element in an array and to compute
 * the average of all the values in the array.
 * You will see how DataSpaces accesses the values without reading from disk.
 */
#include "dspaces.h"
#include "mpi.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
// Example using array size, 128.
// If modifying, MUST change in minmax_writer.c as well.
#define ARRAY_SIZE 128

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

    // Name our data.
    char var_name[128];
    sprintf(var_name, "ex3_sample_data");

    // Each process will need to compute its DataSpace index
    int tasks_per_proc = ARRAY_SIZE / nprocs;
    int tasks_left_over = ARRAY_SIZE % nprocs;
    int ds_lb_index, ds_ub_index;

    if(rank < tasks_left_over) {
        ds_lb_index = rank * (tasks_per_proc + 1);
        ds_ub_index = ds_lb_index + tasks_per_proc;
    } else {
        ds_lb_index = rank * tasks_per_proc + tasks_left_over;
        ds_ub_index = ds_lb_index + tasks_per_proc - 1;
    }

    // Allocate temporary buffer for computing min/max
    int *tempDataBuffer = malloc((ds_ub_index - ds_lb_index + 1) * sizeof(int));

    // Define the dimensionality of the data to be received
    int ndim = 1;

    // Prepare LOWER and UPPER bound dimensions
    uint64_t lb = ds_lb_index, ub = ds_ub_index;

    // DataSpaces: Get data array from the space
    // Usage: dspaces_get(Name of variable, version num,
    // size (in bytes of each element), dimensions for bounding box,
    // lower bound coordinates, upper bound coordinates,
    // ptr to data buffer
    dspaces_get(client, var_name, 1, sizeof(int), ndim, &lb, &ub,
                tempDataBuffer, -1);

    int i;
    int array_size = sizeof(tempDataBuffer) / sizeof(int);
    int local_max = tempDataBuffer[0];
    int local_min = tempDataBuffer[0];
    int sum = 0; // for avg

    // Find Max and Min in our local buffer
    // Also, sum the contents of this buffer for averaging purposes
    for(i = 0; i < array_size; i++) {

        sum += tempDataBuffer[i];

        if(local_max < tempDataBuffer[i]) {
            local_max = tempDataBuffer[i];
        } else if(local_min > tempDataBuffer[i]) {
            local_min = tempDataBuffer[i];
        }
    }

    int local_avg = sum / i;

    free(tempDataBuffer);

    int global_max, global_min, global_avg;

    // Reduce all local maximums to find the overall maximum in the data
    MPI_Reduce(&local_max, &global_max, 1, MPI_INT, MPI_MAX, 0, gcomm);
    // Reduce all local minimums to find the overall minimum in the data
    MPI_Reduce(&local_min, &global_min, 1, MPI_INT, MPI_MIN, 0, gcomm);
    // Reduce all local avgs into a global sum, then divide by the number
    // of processes to get the global average
    MPI_Reduce(&local_avg, &global_avg, 1, MPI_INT, MPI_SUM, 0, gcomm);
    global_avg = global_avg / nprocs;

    // Report data to user
    if(rank == 0) {
        printf("Max: %d, Min: %d, Average: %d\n", global_max, global_min,
               global_avg);
    }

    // Signal the server to shutdown (the server must receive this signal n
    // times before it shuts down, where n is num_apps in dataspaces.conf)
    dspaces_kill(client);

    // DataSpaces: Finalize and clean up DS process
    dspaces_fini(client);

    MPI_Barrier(gcomm);
    MPI_Finalize();

    return 0;
}
