# Authors: Wolf Honore, Victor Liu, Ka Wo Hong
# Assignment: CSC 258 Project (Spring 2016)

HADOOP_HOME = /u/cs258/hadoop

CC = mpicc
CCFLAGS = -Wall
LDFLAGS = -I$(HADOOP_HOME)/include -L$(HADOOP_HOME)/lib/native -lhdfs

OBJS = nowsort

all: $(OBJS)

nowsort: nowsort.c
	$(CC) $(CCFLAGS) $(LDFLAGS) -o $@ $^

clean:
	rm -f *.o $(OBJS)
