/*
* This file is part of cola
* Copyright (c) 2013 Gianni Tedesco
* This program is released under the terms of the GNU GPL version 2
*/
#ifndef _COLA_COMMON_H
#define _COLA_COMMON_H

#include <stdint.h>
#include <inttypes.h>

#include "compiler.h"

typedef uint64_t cola_key_t;
typedef uint64_t cola_val_t;

static inline int cola_parse_key(const char *str, cola_key_t *val)
{
	char *end;

	*val = strtoull(str, &end, 0);
	if ( end == str || *end != '\0' )
		return 0;

	return 1;
}

#endif /* _COLA_COMMON_H */
