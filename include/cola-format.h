#ifndef _COLA_FORMAT_H
#define _COLA_FORMAT_H

#include "cola-common.h"

#define COLA_MAGIC (0xc0U | (0x00U << 8) | ('L' << 16) | (('A') << 24))
#define COLA_CURRENT_VER 0
struct cola_hdr {
	cola_key_t h_nelem;
	uint32_t h_magic;
	uint32_t h_vers;
} _packed;

struct cola_elem {
	cola_key_t key;
//	cola_val_t val;
} _packed;

#endif /* _COLA_FORMAT_H */
