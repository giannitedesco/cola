/*
* This file is part of cola
* Copyright (c) 2013 Gianni Tedesco
* This program is released under the terms of the GNU GPL version 2
*/
#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/mman.h>

#include <cola.h>
#include <cola-format.h>
#include <os.h>

#define INITIAL_LEVELS		17 /* 128K */
#define MAP_LEVELS		23 /* 8M */

#define DEBUG 0
#if DEBUG
#define dprintf(x, ...)  printf("\033[35m" x "\033[0m", ##__VA_ARGS__)
#else
#define dprintf(x...) do {} while(0)
#endif

struct _cola {
	int c_fd;
	cola_key_t c_nelem;
	uint8_t *c_map;
	size_t c_mapsz;
	unsigned int c_maplvls;
};

static int remap(struct _cola *c, unsigned int lvlno)
{
	size_t sz;
	uint8_t *map;

	printf("remap %u\n", lvlno);

	sz = (1U << (lvlno + 1)) - 1;
	sz *= sizeof(struct cola_elem);
	sz += sizeof(struct cola_hdr);

	map = mremap(c->c_map, c->c_mapsz, sz, MREMAP_MAYMOVE);
	if ( map == MAP_FAILED ) {
		fprintf(stderr, "%s: mmap: %s\n", cmd, os_err());
		return 0;
	}

	c->c_maplvls = lvlno;
	c->c_mapsz = sz;
	c->c_map = map;
	return 1;
}

static int map(struct _cola *c)
{
	size_t sz;
	uint8_t *map;

	printf("map %u\n", INITIAL_LEVELS);

	sz = (1U << (INITIAL_LEVELS + 1)) - 1;
	sz *= sizeof(struct cola_elem);
	sz += sizeof(struct cola_hdr);

	map = mmap(NULL, sz, PROT_READ|PROT_WRITE, MAP_SHARED, c->c_fd, 0);
	if ( map == MAP_FAILED ) {
		fprintf(stderr, "%s: mmap: %s\n", cmd, os_err());
		return 0;
	}

	c->c_maplvls = INITIAL_LEVELS;
	c->c_mapsz = sz;
	c->c_map = map;
	return 1;
}

static struct _cola *do_open(const char *fn, int rw, int create, int overwrite)
{
	struct _cola *c = NULL;
	struct cola_hdr hdr;
	size_t sz;
	int eof, oflags;

	c = calloc(1, sizeof(*c));
	if ( NULL == c )
		goto out;

	if ( create ) {
		oflags = O_RDWR | O_CREAT | ((overwrite) ? O_TRUNC : O_EXCL);
	}else{
		oflags = (rw) ? O_RDWR : O_RDONLY;
	}

	c->c_fd = open(fn, oflags, 0644);
	if ( c->c_fd < 0 ) {
		fprintf(stderr, "%s: open: %s: %s\n", cmd, fn, os_err());
		goto out_free;
	}

	if ( create ) {
		off_t initial;

		hdr.h_nelem = 0;
		hdr.h_magic = COLA_MAGIC;
		hdr.h_vers = COLA_CURRENT_VER;
		if ( !fd_write(c->c_fd, &hdr, sizeof(hdr)) ) {
			fprintf(stderr, "%s: write: %s: %s\n",
				cmd, fn, os_err());
			goto out_close;
		}

		initial = (1U << (INITIAL_LEVELS + 1)) - 1;
		initial *= sizeof(struct cola_elem);
		initial += sizeof(hdr);
		if ( posix_fallocate(c->c_fd, 0, initial) ) {
			fprintf(stderr, "%s: %s: fallocate: %s\n",
				cmd, fn, os_err());
		}
	}else{
		sz = sizeof(hdr);
		if ( !fd_read(c->c_fd, &hdr, &sz, &eof) || sz != sizeof(hdr) ) {
			fprintf(stderr, "%s: read: %s: %s\n",
				cmd, fn, os_err2("File truncated"));
			goto out_close;
		}

		if ( hdr.h_magic != COLA_MAGIC ) {
			fprintf(stderr, "%s: %s: Bad magic\n", cmd, fn);
			goto out_close;
		}

		if ( hdr.h_vers != COLA_CURRENT_VER ) {
			fprintf(stderr, "%s: %s: Unsupported vers\n", cmd, fn);
			goto out_close;
		}

		c->c_nelem = hdr.h_nelem;
	}

	if ( !map(c) )
		goto out_close;

	/* success */
	goto out;

out_close:
	fd_close(c->c_fd);
out_free:
	free(c);
	c = NULL;
out:
	return c;
}

cola_t cola_open(const char *fn, int rw)
{
	return do_open(fn, rw, 0, 0);
}

cola_t cola_creat(const char *fn, int overwrite)
{
	return do_open(fn, 1, 1, overwrite);
}

static struct cola_elem *read_level_part(struct _cola *c, unsigned int lvlno,
					cola_key_t from, cola_key_t to)
{
	struct cola_elem *level = NULL;
	cola_key_t nr_ent, ofs;
	size_t sz;
	int eof;

	assert(from <= to);
	assert(to <= (1U << lvlno));

	nr_ent = to - from;
	ofs = (1U << lvlno) - 1;
	ofs += from;
	ofs *= sizeof(*level);
	ofs += sizeof(struct cola_hdr);
	sz = nr_ent * sizeof(*level);

	if ( lvlno > c->c_maplvls ) {
		level = malloc(sz);
		if ( NULL == level )
			goto out;

		if ( !fd_pread(c->c_fd, ofs, level, &sz, &eof) ||
				sz != (nr_ent * sizeof(*level)) ) {
			fprintf(stderr, "%s: read: %s\n",
				cmd, os_err2("File truncated"));
			goto out_free;
		}
	}else{
		level = (struct cola_elem *)(c->c_map + ofs);
	}

	goto out; /* success */

out_free:
	free(level);
	level = NULL;
out:
	return level;
}

static struct cola_elem *read_level(struct _cola *c, unsigned int lvlno)
{
	return read_level_part(c, lvlno, 0, 1U << lvlno);
}

static int write_level(struct _cola *c, unsigned int lvlno,
			struct cola_elem *level)
{
	cola_key_t nr_ent, ofs;
	size_t sz;
	int ret;

	nr_ent = (1 << lvlno);
	sz = nr_ent * sizeof(*level);
	ofs = nr_ent - 1;
	ofs *= sizeof(*level);
	ofs += sizeof(struct cola_hdr);

	if ( (1U << lvlno) > c->c_nelem ) {
		if ( lvlno > c->c_maplvls ) {
			printf("fallocate level %u\n", lvlno);
			if ( posix_fallocate(c->c_fd, ofs, ofs + sz) )
				fprintf(stderr, "%s: fallocate: %s\n",
					cmd, os_err());
		}
	}

	if ( lvlno > c->c_maplvls ) {
		ret = fd_pwrite(c->c_fd, ofs, level, sz);
	}else{
		struct cola_elem *out;
		out = (struct cola_elem *)(c->c_map + ofs);
		memcpy(out, level, sz);
		ret = 1;
	}

	/* TODO: remap then write async */
	if ( lvlno > INITIAL_LEVELS &&
			lvlno < MAP_LEVELS &&
			(1U << lvlno) > c->c_nelem ) {
		ret = remap(c, lvlno);
	}
	return ret;
}

/* a is always what was in the k-1'th array */
static struct cola_elem *level_merge(struct cola_elem *a,
					struct cola_elem *b,
					unsigned int lvlno)
{
	cola_key_t mcnt = (1 << (lvlno + 1));
	cola_key_t max = (1 << lvlno);
	struct cola_elem *m;
	cola_key_t i, aa, bb;

	m = malloc(sizeof(*m) * mcnt);
	if ( NULL == m )
		return NULL;

	for(i = aa = bb = 0; i < mcnt; i++) {
		if ( aa < max && a[aa].key < b[bb].key ) {
			m[i] = a[aa];
			aa++;
		}else if ( bb < max && a[aa].key > b[bb].key ) {
			m[i] = b[bb];
			bb++;
		}else{
			if ( aa < bb ) {
				m[i] = a[aa];
				aa++;
			}else{
				m[i] = b[bb];
				bb++;
			}
		}
	}

	assert(aa == (1U << lvlno));
	assert(bb == (1U << lvlno));

#if 0
	/* modify a with ideal keys and pointers, a will
	 * always be 'empty' after this merge
	 *
	 * FIXME: changing keys ruins prior level pointers
	*/
	for(i = 0; i < max; i++) {
		a[i].key = m[i << 1U].key;
		a[i].fp = i << 1U;
	}
#else
	for(i = bb = 0; i < max; i++) {
		while(bb < mcnt && m[bb].key < a[i].key) {
			bb++;
		}
		a[i].fp = bb;
	}
#endif
	return m;
}

static int fractional_cascade(struct _cola *c, unsigned int lvlno,
				struct cola_elem *cur)
{
	unsigned int nl = lvlno + 1;
	struct cola_elem *next;
	cola_key_t i, j;

	if ( (c->c_nelem < (1U << nl)) )
		return 1;

	/* TODO: investigate relying on prior level pointers
	 * if that level went from 1 to 0.
	*/
	dprintf(" - fractional cascade %u -> %u\n", lvlno, nl);
	next = read_level(c, nl);
	if ( NULL == next )
		return 0;

	for(i = j = 0; i < (1U << lvlno); i++) {
		while(j < (1U << nl) && next[j].key < cur[i].key) {
			j++;
		}
		cur[i].fp = j;
	}
	if ( nl > c->c_maplvls )
		free(next);

	return 1;
}

int cola_insert(cola_t c, cola_key_t key)
{
	cola_key_t newcnt = c->c_nelem + 1;
	struct cola_elem *level;
	unsigned int i;

	level = calloc(1, sizeof(*level));
	if ( NULL == level )
		return 0;

	dprintf("Insert key %"PRIu64"\n", key);
	level->key = key;

	for(i = 0; newcnt >= (1U << i); i++) {
		if ( c->c_nelem & (1U << i) ) {
			struct cola_elem *level2, *merged;
			dprintf(" - level %u full\n", i);
			level2 = read_level(c, i);
			if ( NULL == level2 ) {
				free(level);
				return 0;
			}
			merged = level_merge(level2, level, i);
			if ( !write_level(c, i, level2) ) {
				/* FIXME */
			}
			if ( i > c->c_maplvls )
				free(level2);
			free(level);
			level = merged;
			if ( NULL == merged )
				return 0;
		}else{
			dprintf(" - level %u empty\n", i);
			if ( !fractional_cascade(c, i, level) ||
					!write_level(c, i, level) ) {
				free(level);
				return 0;
			}
			free(level);
			break;
		}
	}

	c->c_nelem++;
	dprintf("\n");
#if DEBUG
	cola_dump(c);
	dprintf("\n");
#endif
	return 1;
}

static int query_level(struct _cola *c, cola_key_t key,
			unsigned int lvlno, int *result,
			cola_key_t *lo, cola_key_t *hi)
{
	struct cola_elem *level, *p;
	cola_key_t n, l, h, sz;

	dprintf("bsearch level %u (%"PRIu64":%"PRIu64")\n", lvlno, *lo, *hi);
	level = read_level_part(c, lvlno, *lo, *hi);
	if ( NULL == level )
		return 0;

	sz = *hi - *lo;
	l = 0;
	h = (1U << (lvlno + 1));

	for(*result = 0, p = level, n = sz; n; ) {
		cola_key_t i = n / 2;
		if ( key < p[i].key ) {
			if ( p[i].fp < h )
				h = p[i].fp;
			n = i;
		}else if ( key > p[i].key ) {
			if ( p[i].fp > l )
				l = p[i].fp;
			p = p + (i + 1);
			n = n - (i + 1);
		}else{
			*result = 1;
			break;
		}
	}

	if ( *result == 0 ) {
		dprintf(" - nope %"PRIu64" @ %"PRIu64"\n", n, p - level);
		dprintf(" - lo=%"PRIu64" hi=%"PRIu64"\n", l, h);
	}

	*lo = l;
	*hi = h;
	free(level);
	return 1;
}

int cola_query(cola_t c, cola_key_t key, int *result)
{
	cola_key_t lo = 0, hi = 1;
	unsigned int i;

	for(i = 0; c->c_nelem >= (1U << i); i++) {
		if ( !query_level(c, key, i, result, &lo, &hi) )
			return 0;
		if ( *result )
			return 1;
	}

	*result = 0;
	return 1;
}

int cola_dump(cola_t c)
{
	unsigned int i;

	for(i = 0; c->c_nelem >= (1U << i); i++) {
		struct cola_elem *level;
		unsigned int j;

		level = read_level(c, i);
		if ( NULL == level )
			return 0;

		if ( !(c->c_nelem & (1U << i)) )
			printf("\033[2;37m");
		printf("level %u:", i);
		for(j = 0; j < (1U << i); j++) {
			printf(" %"PRIu64, level[j].key);
			printf("[%"PRIu64"]", level[j].fp);
		}
		if ( !(c->c_nelem & (1U << i)) )
			printf("\033[0m");
		printf("\n");
		free(level);
	}

	return 1;
}

int cola_close(cola_t c)
{
	int ret = 1;
	if ( c ) {
		struct cola_hdr *hdr = (struct cola_hdr *)c->c_map;
		hdr->h_nelem = c->c_nelem;
		hdr->h_magic = COLA_MAGIC;
		hdr->h_vers = COLA_CURRENT_VER;
		if ( msync(c->c_map, c->c_mapsz, MS_ASYNC) ) {
			ret = 0;
		}
		if ( munmap(c->c_map, c->c_mapsz) ) {
			ret = 0;
		}
		if ( !fd_close(c->c_fd) ) {
			ret = 0;
		}
		free(c);
	}
	return ret;
}
