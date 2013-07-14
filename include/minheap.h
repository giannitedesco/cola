/*
* This file is part of cola
* Copyright (c) 2013 Gianni Tedesco
* This program is released under the terms of the GNU GPL version 3
*/
#ifndef _MINHEAP_H
#define _MINHEAP_H

#include "cola-common.h"

struct heap_item {
	cola_key_t key;
	unsigned long val;
};

void minheap_init(unsigned long nr_items,
			struct heap_item h[static nr_items + 1]);
void minheap_sift_down(unsigned long nr_items,
			struct heap_item h[static nr_items + 1]);
void minheap_sift_up(unsigned long nr_items,
			struct heap_item h[static nr_items + 1]);

#endif /* _MINHEAP_H */
