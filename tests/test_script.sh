#!/bin/bash
## Create dataspaces configuration file
echo "## Config file for DataSpaces
ndim = 3
dims = 256,256,256
max_versions = 8
max_readers = 8
lock_type = 2
" > dataspaces.conf
if [ $1 -eq 1 ]; then
	rm -rf servids.0
	mpirun -n 2 ./dspaces_server sockets &
	sleep 5
	mpirun -n 1 ./test_writer sockets 1 1 128 5 8
	pkill dspaces_server
elif [ $1 -eq 2 ]; then
	rm -rf servids.0
	mpirun -n 2 ./dspaces_server sockets &
	sleep 5
	mpirun -n 2 ./test_writer sockets 1 2 64 5 8 &
	sleep 2
	mpirun -n 2 ./test_reader sockets 1 2 64 5 8
	pkill dspaces_server
	pkill test_writer
elif [ $1 -eq 3 ]; then
	rm -rf servids.0
	mpirun -n 2 ./dspaces_server sockets &
	sleep 5
	mpirun -n 2 ./test_writer sockets 1 2 64 5 8 &
	sleep 2
	mpirun -n 2 ./test_reader sockets 1 2 32 5 8
	pkill dspaces_server
	pkill test_writer
elif [ $1 -eq 4 ]; then
	rm -rf servids.0
	mpirun -n 2 ./dspaces_server sockets &
	sleep 5
	mpirun -n 2 ./test_writer sockets 1 2 64 5 8 &
	sleep 2
	mpirun -n 2 ./test_reader sockets 1 2 64 2 8
	pkill dspaces_server
	pkill test_writer
fi