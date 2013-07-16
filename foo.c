#include <stdio.h>
#include <stdint.h>

volatile unsigned int result;

static inline
unsigned int log2_ceil(unsigned int x)
{
	unsigned int log2 = 32 - ( __builtin_clz(x) + 1 );
	unsigned int y = x & (x - 1);
	unsigned int off = (-y & (1 << 31)) >> 31;
	return log2 + off;
}

static inline
unsigned int log2_ceil32(uint64_t val)
{
	uint32_t ival1 = __builtin_clz(val);
	uint32_t ival2 = ival1 + 1;
	uint32_t ret = val << ival2;
	if ( __builtin_expect(ret, 0) )
		return 32 - ival1;
	else
		return 32 - ival2;
}
static inline uint64_t rdtsc(void)
{
	uint32_t lo, hi;
	asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
	return (uint64_t)hi << 32 | lo;
}

#define ITER 30
int main(int argc, char **argv)
{
	unsigned int i;
	uint64_t t0, t1, t2;

	t0 = rdtsc();
	for(i = 0; i < (1 << ITER); i++) {
		result = log2_ceil32(i);
	}

	t1 = rdtsc();
	for(i  = 0; i < (1 << ITER); i++) {
		result = log2_ceil32(i);
	}

	t2 = rdtsc();
	printf("A -> %f cycles\n",
			((double)t1 - (double)t0) / (double)(1 << ITER));
	printf("B -> %f cycles\n",
			((double)t2 - (double)t1) / (double)(1 << ITER));

	return 0;
}
