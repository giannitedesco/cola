/*
* This file is part of cola
* Copyright (c) 2013 Gianni Tedesco
* This program is released under the terms of the GNU GPL version 2
*/
#ifndef _COLA_FORMAT_H
#define _COLA_FORMAT_H

#include "cola-common.h"

#define COLA_MAGIC (0xc0U | (0x00U << 8) | ('L' << 16) | (('A') << 24))

#define COLA_CURRENT_VER 1
/* version 0: basic COLA
 * version 1: fractional cascading
*/
struct cola_hdr {
	cola_key_t h_nelem; /* number of keys */
	uint32_t h_magic;
	uint32_t h_vers;
} _packed;

struct cola_elem {
	cola_key_t key;
} _packed;

#endif /* _COLA_FORMAT_H */
