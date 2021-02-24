# DataSpaces 2.0

This package contains the source code and test applications for
DataSpaces run-time library, which is a flexible interaction and
coordination framework to support coupled applications.

DataSpaces is a distributed framework built on a dynamic set of nodes
of a HPC cluster, which implements a virtual shared-space abstraction
that can be accessed concurrently by all applications in a coupled
simulation workflow. The coupled applications can coordinate and
interact using DataSpaces by inserting and fetching scientific data
of interest through the distributed in-memory space.

DataSpaces provides a simple, yet powerful interface similar to the
tuple-space model, e.g., dspaces_put()/dspaces_get() to insert/retrieve data
objects into/from the virtual space.

## Dependencies

* mercury (git clone --recurse-submodules https://github.com/mercury-hpc/mercury.git)
* argobots (git clone https://github.com/pmodels/argobots.git)
* margo (git clone https://xgitlab.cels.anl.gov/sds/margo.git)
* MPI (for building the storage server binary and test utilities)

## Building
Create a build directory
```
$ mkdir build
$ cd build
```
Now install the dspaces provider
```
$ make
$ make install
```

## APIs

Please read header files include/dspaces.h for detailed API documentation. 

Some notable differences from the orignal DataSpaces API:

DataSpaces no longer requires an MPI communicator or appid be passed to `dspaces_init()`. Additionally, dspaces_init returns a libary handle that must be passed to other API calls.

`dspaces_get()` has now takes a timeout value. This value can be `0` or `-1`. If set to `0`, the user must ensure that the data being read has been written through external means. If timeout is set to `-1`, then the reader will wait for all the requested data to be written in the style of a data subscription.

`dspaces_put_local()` stashed staged data in the writing process' memory space, rather than pushing it to the server. This is analogous to DIMES in original DataSpaces. `dspaces_put()` and `dspaces_put_local()` can be used together.

`dspaces_lock` operations have been removed.

(*NEW in 2.1*): pub/sub operation: specify a data range and a callback to perform upon that data range. See the `test_sub` utility for example usage.

(*NEW in 2.1*): metadata functionality: post unstructured buffers indexed by name/version, with various access modes. 

(*NEW in 2.1*): `dspaces_aget()` allocates the data buffer instead of requiring a properly sized buffer be allocated.

## DataSpaces usage

Setting the `DSPACES_DEBUG` environment variable generates debugging output.

### Running the Server Binary

The server binary, `dspaces_server`, takes a single argument: the listen_address. This is a Mercury-specific connection string (see Mercury documentation for details.) Common values are: `sockets` to use TCP for communication, `sm` for shared memory (if all clienta and server processes are on the same node) and `ofi+X` for RDMA, where `X` is `verbs`, `psm2`, or `cray` as is appropriate for the system fabric.

The server writes the `conf.ds` file, which provides connectivity bootstrapping data to the clients. Clients must be run with this file in their working directory.

Several environment variables influence server behavior:

`DSPACES_DEFAULT_NUM_HANDLERS` - the number of request handling threads launched by the server (in addition to the main thread). Default: 4.
`DSPACES_DRAIN` (EXPERIMENTAL) - asynchronously drain data written to local process spaces using `dspaces_put_local()` to the server. Off by default.

#### dataspaces.conf

The DataSpaces server expects to find the `dataspaces.conf` file in the working directory of the server. This is a list of configuration variables of the format

`<variable> = <value>`, e.g.
`num_apps = 1`

(*NEW in 2.0*): `num_apps`: this value is the number of `dspaces_kill()` calls that are needed to kill the server binary.

`ndim`: number of dimensions for the default global data domain.

`dims`: size of each dimension for the default global data domain.

`max_versions`: maximum number of versions of a data object to be cached in DataSpaces servers.



### The terminator utility

DataSpaces provides a terminator utility for sending a kill signal to the server. Effectively, the terminator joins the server, sends the kill signal, and disconnects. This can be useful for workflows in which it's not clear from inside each application when the workflow should be stopped.

### Example DataSpaces programs

`test_writer`, `test_reader`, and `test_sub` are example DataSpaces client programs. `test_writer` iteratively writes variables into DataSpaces storage and `test_reader` reads those variables, checking for data corruption. `test_sub` performs the same function as `test_reader`, but using the pub/sub mode of operation.

#### test_writer usage:
```
test_writer <dims> np[0] .. np[dims-1] sp[0] ... sp[dims-1] <timesteps> [-s <elem_size>] [-m (server|local)] [-c <var_count>] [-t]
   dims              - number of data dimensions. Must be at least one
   np[i]             - the number of processes in the ith dimension. The product of np[0],...,np[dim-1] must be the number of MPI ranks
   sp[i]             - the per-process data size in the ith dimension
   timesteps         - the number of timestep iterations written
   -s <elem_size>    - the number of bytes in each element. Defaults to 8
   -m (server|local) - the storage mode (stage to server or stage in process memory). Defaults to server
   -c <var_count>    - the number of variables written in each iteration. Defaults to one
   -t                - send server termination after writing is complete
```

#### test_reader usage:
```
test_reader <dims> np[0] .. np[dims-1] sp[0] ... sp[dims-1] <timesteps> [-s <elem_size>] [-c <var_count>] [-t]
   dims              - number of data dimensions. Must be at least one
   np[i]             - the number of processes in the ith dimension. The product of np[0],...,np[dim-1] must be the number of MPI ranks
   sp[i]             - the per-process data size in the ith dimension
   timesteps         - the number of timestep iterations written
   -s <elem_size>    - the number of bytes in each element. Defaults to 8
   -c <var_count>    - the number of variables written in each iteration. Defaults to one
   -t                - send server termination signal after reading is complete
   -a                - ask dataspaces to allocate read data buffer
```

#### test_sub usage
```
test_sub <dims> np[0] .. np[dims-1] sp[0] ... sp[dims-1] <timesteps> [-s <elem_size>] [-c <var_count>] [-t]
   dims              - number of data dimensions. Must be at least one
   np[i]             - the number of processes in the ith dimension. The product of np[0],...,np[dim-1] must be the number of MPI ranks
   sp[i]             - the per-process data size in the ith dimension
   timesteps         - the number of timestep iterations written
   -s <elem_size>    - the number of bytes in each element. Defaults to 8
   -c <var_count>    - the number of variables written in each iteration. Defaults to one
   -t                - send server termination signal after reading is complete
```
 
 
