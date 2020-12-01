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
===============

Please read header files include/dspaces.h for detailed API documentation. 

Some notable differences from the orignal DataSpaces API:

DataSpaces no longer requires an MPI communicator or appid be passed to `dspaces_init()`. 

