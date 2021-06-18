from dspaces_wrapper import *
import numpy as np

class dspaces:
    def __init__(self, rank = None, comm = None):
        if rank == None:
            from mpi4py import MPI
            if comm == None:
                comm = MPI.COMM_WORLD
            rank = comm.Get_rank()
        self.client = wrapper_dspaces_init(rank)

    def __del__(self):
        wrapper_dspaces_fini(self.client) 
 
    def kill_server(self, token_count = 1):
        for token in range(token_count):
            wrapper_dspaces_kill(self.client)

    def put(self, data, name, version, offset):
        if len(offset) != len(data.shape):
            raise TypeError("offset should have the same dimensionality as data")
        wrapper_dspaces_put(self.client, data, name.encode('ascii'), version, offset)

    def get(self, name, version, lb, ub, dtype, timeout):
        if len(lb) != len(ub):
            raise TypeError("lower-bound and upper-bound must have the same dimensionality")
        return wrapper_dspaces_get(self.client, name.encode('ascii'), version, lb, ub, np.dtype(dtype), timeout)    

