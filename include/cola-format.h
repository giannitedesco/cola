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

	/* fractional pointer: where this key would be
	 * if it were inserted in to the level k + 1
	 * index in to that array so at least top 4 bits
	 * are free if we need to store a few flags later.
	*/
	cola_key_t fp;

	/* offsets to prior and next fractional item from level
	 * k - 1 - hopefully prev/next less than 80GB or so apart
	*/
//	uint32_t prev;
//	uint32_t next;
//	cola_val_t val;
} _packed;

#endif /* _COLA_FORMAT_H */
