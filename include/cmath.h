/*
* This file is part of cola
* Copyright (c) 2013 Gianni Tedesco
* This program is released under the terms of the GNU GPL version 3
*/
#ifndef _CMATH_H
#define _CMATH_H

#include <bits/wordsize.h>

#if __WORDSIZE == 64
# define ctz64(x) __builtin_ctzl(x)
# define ctz32(x) __builtin_ctz(x)
# define clz64(x) __builtin_clzl(x)
# define clz32(x) __builtin_clz(x)
#define log2_floor(x) log2_floor64(x)
#define log2_ciel(x) log2_ciel64(x)
#elif __WORDSIZE == 32
# define ctz64(x) __builtin_ctzll(x)
# define ctz32(x) __builtin_ctzl(x)
# define clz64(x) __builtin_clzll(x)
# define clz32(x) __builtin_clzl(x)
#define log2_floor(x) log2_floor32(x)
#define log2_ciel(x) log2_ciel32(x)
#elif __WORDSIZE == 32
#else
# error "Bad machine word size"
#endif

static inline unsigned int log2_floor64(uint64_t val)
{
	return 64 - (clz64(val) + 1);
}

static inline unsigned int log2_ceil64(uint64_t val)
{
	uint64_t ival1 = clz64(val);
	uint64_t ival2 = ival1 + 1;
	uint64_t ret = val << ival2;
	if ( ret )
		return 64 - ival1;
	else
		return 64 - ival2;
}

static inline unsigned int log2_floor32(uint32_t val)
{
	return 32 - (clz32(val) + 1);
}

static inline unsigned int log2_ceil32(uint32_t val)
{
	uint32_t ival1 = clz32(val);
	uint32_t ival2 = ival1 + 1;
	uint32_t ret = val << ival2;
	if ( ret )
		return 32 - ival1;
	else
		return 32 - ival2;
}

#endif /* _CMATH_H */
