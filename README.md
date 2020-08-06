# dspaces

## Dependencies

* mercury (git clone --recurse-submodules https://github.com/mercury-hpc/mercury.git)
* argobots (git clone https://github.com/pmodels/argobots.git)
* margo (git clone https://xgitlab.cels.anl.gov/sds/margo.git)
* ssg 

## Building
Create a build directory
```
$ mkdir build
$ cd build
```
You need MPI compiler to build spaces
```
$ cmake -DENABLE_TESTS=ON -DCMAKE_C_COMPILER=mpicc ..
```
Now install the dspaces provider
```
$ make
$ make install
```

