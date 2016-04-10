/*
 * Authors: Wolf Honore, Victor Liu, Ka Wo Hong
 * Assignment: CSC 258 Project (Spring 2016)
 *
 * Description: Implementation of quicksort. 
 * Input: integer array
 */
#include <stdio.h>
#include <stdlib.h>

int quicksort(int *data, int begin, int end){ 
	int pivot = data[begin];
	while (begin < end){ 
		while(begin < end && data[end] >= pivot)
			end--;
		data[begin] = data[end];
		while(begin < end && data[begin] <= pivot)
			begin++;
		data[end] = data[begin];
	}
	data[begin] = pivot;
	return(begin);
}

void sort(int *data, int begin, int end){ 
	int mid = 0;
	if(begin >= end)
		return;
	mid = quicksort(data,begin,end);
	sort(data, begin, mid-1);
	sort(data, mid+1, end);
}

int main(int argc, char *argv[]){
	int data[10]= {7, 4, 13, 54, 76, 19, 546, 88, 16, 25};
	int i;
	sort(data, 0, 9);
	for (i = 0; i < 10; i++){
		printf("%d ", data[i]);
	}
	return(0);
}
