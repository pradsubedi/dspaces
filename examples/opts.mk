CC=mpicc
OPTS=-g
DSPACES_INC=$(shell pkg-config --cflags dspaces)
DSPACES_LIBS=$(shell pkg-config --libs dspaces)
CFLAGS=$(OPTS) $(DSPACES_INC)
LDFLAGS=$(DSPACES_LIBS)
