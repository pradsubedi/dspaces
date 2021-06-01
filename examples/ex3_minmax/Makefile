include ../opts.mk

all: minmaxavg_reader minmaxavg_writer

minmaxavg_reader: minmaxavg_reader.o
	$(CC) -o $@ $< $(LDFLAGS)

minmaxavg_writer: minmaxavg_writer.o
	$(CC) -o $@ $< $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f minmaxavg_reader minmaxavg_writer *.o
