# Authors: Wolf Honore, Victor Liu, Ka Wo Hong
# Assignment: CSC 258 Project (Spring 2016)

CC = mpicc
CCFLAGS = -Wall

OBJS = nowsort

all: $(OBJS)

nowsort: nowsort.c
	$(CC) $(CCFLAGS) -o $@ $^

clean:
	rm -f *.o $(OBJS)
