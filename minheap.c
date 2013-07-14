/*
* This file is part of cola
* Copyright (c) 2013 Gianni Tedesco
* This program is released under the terms of the GNU GPL version 3
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <minheap.h>
#include <cmath.h>
#include <assert.h>

static unsigned long parent(unsigned long idx)
{
	return idx >> 1;
}

static unsigned long left(unsigned long idx)
{
	return idx << 1;
}

static unsigned long right(unsigned long idx)
{
	return (idx << 1) + 1;
}

static void do_sift_up(unsigned long idx, unsigned long nr_items,
				struct heap_item h[static nr_items + 1])
{
	struct heap_item tmp;
	unsigned long pidx;
	cola_key_t pv, v;

	assert(idx > 0);

	if ( idx == 1 )
		return;

	v = h[idx].key;
	pidx = parent(idx);
	pv = h[pidx].key;
	if ( pv < v )
		return;

	tmp = h[pidx];
	h[pidx] = h[idx];
	h[idx] = tmp;
	do_sift_up(pidx, nr_items, h);
}

static void do_sift_down(unsigned long idx, unsigned long nr_items,
				struct heap_item h[static nr_items + 1])
{
	unsigned long smallest, l, r;
	struct heap_item tmp;

	l = left(idx);

	if ( l > nr_items )
		return;

	r = right(idx);
	if ( r > nr_items ) {
		smallest = l;
	}else{
		smallest = (h[l].key < h[r].key) ? l : r;
	}

	if ( h[idx].key < h[smallest].key )
		return;

	tmp = h[smallest];
	h[smallest] = h[idx];
	h[idx] = tmp;
	do_sift_down(smallest, nr_items, h);
}

void minheap_init(unsigned long nr_items,
			struct heap_item h[static nr_items + 1])
{
	unsigned int i, n = (nr_items + 1) >> 1;

	for(i = nr_items; i > nr_items - n; i--) {
		do_sift_up(i, nr_items, h);
	}
}

void minheap_sift_down(unsigned long nr_items,
				struct heap_item h[static nr_items + 1])
{
	do_sift_down(1, nr_items, h);
}

void minheap_sift_up(unsigned long nr_items,
				struct heap_item h[static nr_items + 1])
{
	do_sift_up(nr_items, nr_items, h);
}

#undef MAIN
#if MAIN
void print_heap(unsigned long nr_items,
				struct heap_item h[static nr_items + 1])
{
	unsigned long i;
	for(i = 0; i < nr_items; i++)
		printf("%"PRId64" ", h[i + 1].key);
	printf("\n");
}

int main(int argc, char **argv)
{
	unsigned long i, nr = atoi(argv[1]);
	struct heap_item *h;

	h = calloc(nr, sizeof(*h));
	h = h - 1;

	for(i = 0; i < nr; i++)
		h[i + 1].key = nr - i;
	printf("init: ");
	print_heap(nr, h);

	minheap_init(nr, h);
	printf("heapified: ");
	print_heap(nr, h);

	while(nr) {
		printf("%"PRId64": ", h[1].key);
		h[1] = h[nr--];
		minheap_sift_down(nr, h);
		print_heap(nr, h);
	}

	printf("\n");

	h[++nr].key = 100;
	minheap_sift_up(nr, h);
	print_heap(nr, h);

	h[++nr].key = 300;
	minheap_sift_up(nr, h);
	print_heap(nr, h);

	h[++nr].key = 200;
	minheap_sift_up(nr, h);
	print_heap(nr, h);

	printf("\n");
	while(nr) {
		printf("%"PRId64": ", h[1].key);
		h[1] = h[nr--];
		minheap_sift_down(nr, h);
		print_heap(nr, h);
	}
	return EXIT_SUCCESS;
}
#endif
