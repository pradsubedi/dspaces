include ../opts.mk

all: put get

put: put.o
	$(CC) -o $@ $< $(LDFLAGS)

get: get.o
	$(CC) -o $@ $< $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f put get *.o
