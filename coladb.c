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
#include <unistd.h>

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
	cola_key_t c_nelem;
	uint8_t *c_map;
	size_t c_mapsz;
	unsigned int c_maplvls;
	unsigned int c_nxtlvl;
	int c_fd;
	int c_rw;
};

struct buf {
	struct cola_elem *ptr;
	cola_key_t nelem;
	int heap;
};

static unsigned int cfls(cola_key_t k)
{
	unsigned int i, ret;
	for(i = ret = 0; i < sizeof(k)/8; i++) {
		cola_key_t tmp;

		tmp = 1U << i;
		if ( tmp > k )
			break;

		if ( k & tmp )
			ret = i;
	}
	return ret;
}

static int remap(struct _cola *c, unsigned int lvlno)
{
	size_t sz;
	uint8_t *map;

	dprintf(" - remap %u\n", lvlno);

	sz = (1U << (lvlno + 2)) - 1;
	sz *= sizeof(struct cola_elem);
	sz += sizeof(struct cola_hdr);

	map = mremap(c->c_map, c->c_mapsz, sz, MREMAP_MAYMOVE);
	if ( map == MAP_FAILED ) {
		fprintf(stderr, "%s: mmap: %s\n", cmd, os_err());
		return 0;
	}

	madvise(map, c->c_mapsz, MADV_RANDOM);
	c->c_maplvls = lvlno;
	c->c_mapsz = sz;
	c->c_map = map;
	return 1;
}

static int map(struct _cola *c)
{
	int f;
	size_t sz;
	uint8_t *map;

	f = (c->c_rw) ? (PROT_READ|PROT_WRITE) : (PROT_READ);
	sz = (1U << (INITIAL_LEVELS + 1)) - 1;
	sz *= sizeof(struct cola_elem);
	sz += sizeof(struct cola_hdr);

	map = mmap(NULL, sz, f, MAP_SHARED, c->c_fd, 0);
	if ( map == MAP_FAILED ) {
		fprintf(stderr, "%s: mmap: %s\n", cmd, os_err());
		return 0;
	}

	madvise(map, sz, MADV_RANDOM);

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

	c->c_rw = rw;
	if ( !map(c) )
		goto out_close;

	c->c_nxtlvl = cfls(c->c_nelem);
	if ( c->c_nxtlvl < INITIAL_LEVELS )
		c->c_nxtlvl = INITIAL_LEVELS + 1;
	dprintf("next level init to %u\n", c->c_nxtlvl);

	/* success */
	goto out;

out_close:
	close(c->c_fd);
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

static void buf_finish(struct buf *buf)
{
	if ( buf->heap )
		free(buf->ptr);
	buf->ptr = NULL;
	buf->nelem = 0;
}

static int buf_alloc(struct _cola *c, unsigned int nelem, struct buf *buf)
{
	buf->ptr = malloc(nelem * sizeof(*buf->ptr));
	if ( NULL == buf->ptr )
		return 0;
	buf->nelem = nelem;
	buf->heap = 1;
	return 1;
}

static int read_level_part(struct _cola *c, unsigned int lvlno,
					cola_key_t from, cola_key_t to,
					struct buf *buf)
{
	cola_key_t nr_ent, ofs;
	int eof;

	assert(from <= to);
	assert(to <= (1U << lvlno));

	nr_ent = to - from;
	ofs = (1U << lvlno) - 1;
	ofs += from;
	ofs *= sizeof(*buf->ptr);
	ofs += sizeof(struct cola_hdr);

	if ( lvlno > c->c_maplvls ) {
		size_t sz;

		buf->ptr = malloc(nr_ent * sizeof(*buf->ptr));
		if ( NULL == buf->ptr )
			return 0;

		buf->nelem = nr_ent;
		buf->heap = 1;

		sz = nr_ent * sizeof(*buf->ptr);
		if ( !fd_pread(c->c_fd, ofs, buf->ptr, &sz, &eof) ||
				sz != (nr_ent * sizeof(*buf->ptr)) ) {
			fprintf(stderr, "%s: read: %s\n",
				cmd, os_err2("File truncated"));
			buf_finish(buf);
			return 0;
		}
	}else{
		buf->ptr = (struct cola_elem *)(c->c_map + ofs);
		buf->nelem = (1U << lvlno);
		buf->heap = 0;
	}

	return 1;
}

static int read_level(struct _cola *c, unsigned int lvlno,
				struct buf *buf)
{
	return read_level_part(c, lvlno, 0, 1U << lvlno, buf);
}

static int write_prep(struct _cola *c, unsigned int lvlno, struct buf *buf)
{
	cola_key_t nr_ent, ofs;

	nr_ent = (1 << lvlno);

	ofs = nr_ent - 1;
	ofs *= sizeof(*buf->ptr);
	ofs += sizeof(struct cola_hdr);

	if ( lvlno > c->c_maplvls ) {
		buf->ptr = malloc(nr_ent);
		if ( NULL == buf->ptr ) {
			fprintf(stderr, "%s: malloc: %s\n", cmd, os_err());
			return 0;
		}
		buf->heap = 1;
	}else{
		buf->ptr = (struct cola_elem *)(c->c_map + ofs);
		buf->heap = 0;
	}

	buf->nelem = nr_ent;
	return 1;
}

static int write_level(struct _cola *c, unsigned int lvlno,
			struct buf *buf)
{
	cola_key_t ofs, nr_ent;
	int ret;

	assert((1U << lvlno) <= buf->nelem);
	nr_ent = (1 << lvlno);
	ofs = nr_ent - 1;
	ofs *= sizeof(*buf->ptr);
	ofs += sizeof(struct cola_hdr);

	assert(nr_ent <= buf->nelem);

	if ( lvlno > c->c_maplvls ) {
		ret = fd_pwrite(c->c_fd, ofs, buf->ptr,
				buf->nelem * sizeof(*buf->ptr));
	}else{
		struct cola_elem *ptr = (struct cola_elem *)(c->c_map + ofs);

		if ( ptr != buf->ptr ) {
			size_t sz;
			sz = nr_ent * sizeof(*buf->ptr);
			memcpy(ptr, buf->ptr, sz);
		}
		ret = 1;
	}

	return ret;
}

/* a is always what was in the k-1'th array */
static void level_merge(struct buf *a, struct buf *b, struct buf *m)
{
	cola_key_t i, aa, bb;

	for(i = aa = bb = 0; i < m->nelem; i++) {
		if ( aa < a->nelem && a->ptr[aa].key < b->ptr[bb].key ) {
			m->ptr[i] = a->ptr[aa];
			aa++;
		}else if ( bb < a->nelem && a->ptr[aa].key > b->ptr[bb].key ) {
			m->ptr[i] = b->ptr[bb];
			bb++;
		}else{
			if ( aa < bb ) {
				assert(aa < a->nelem);
				m->ptr[i] = a->ptr[aa];
				aa++;
			}else{
				assert(bb < b->nelem);
				m->ptr[i] = b->ptr[bb];
				bb++;
			}
		}
	}

	assert(aa == a->nelem);
	assert(bb == b->nelem);
}

int cola_insert(cola_t c, cola_key_t key)
{
	cola_key_t newcnt = c->c_nelem + 1;
	struct buf level;
	unsigned int i;

	dprintf("Insert key %"PRIu64"\n", key);

	if ( !buf_alloc(c, 1, &level) )
		return 0;
	level.ptr[0].key = key;

	/* make sure the level we're about to write to is allocated and,
	 * if required, mapped
	*/
	if ( newcnt == (1ULL << c->c_nxtlvl) ) {
		cola_key_t nr_ent, ofs;
		size_t sz;

		nr_ent = (1ULL << c->c_nxtlvl);
		ofs = nr_ent - 1;
		ofs *= sizeof(struct cola_elem);
		ofs += sizeof(struct cola_hdr);

		sz = nr_ent * sizeof(struct cola_elem);
		dprintf("fallocate level %u\n", c->c_nxtlvl);
		if ( posix_fallocate(c->c_fd, ofs, ofs + sz) )
			fprintf(stderr, "%s: fallocate: %s\n",
				cmd, os_err());
		if ( c->c_nxtlvl <= MAP_LEVELS &&
				(1U << c->c_nxtlvl) > c->c_nelem ) {
			if ( !remap(c, c->c_nxtlvl) )
				return 0;
		}
		c->c_nxtlvl++;
	}

	for(i = 0; newcnt >= (1U << i); i++) {
		if ( c->c_nelem & (1U << i) ) {
			struct buf level2, merged = {0,};
			int ret;

			dprintf(" - level %u full\n", i);
			if ( !read_level(c, i, &level2) ) {
				buf_finish(&level);
				return 0;
			}

			if ( (c->c_nelem & (1U << (i + 1))) ||
					i + 1 >= c->c_maplvls ) {
				ret = buf_alloc(c, (1U << (i + 1)), &merged);
			}else{
				/* landing in next level so write to map */
				ret = write_prep(c, i + 1, &merged);
			}
			if ( !ret ) {
				buf_finish(&level2);
				buf_finish(&level);
				return 0;
			}

			level_merge(&level2, &level, &merged);
			if ( !write_level(c, i, &level2) ) {
				buf_finish(&level2);
				buf_finish(&level);
				buf_finish(&merged);
				return 0;
			}

			buf_finish(&level2);
			buf_finish(&level);

			memcpy(&level, &merged, sizeof(level));
		}else{
			if ( !write_level(c, i, &level) ) {
				buf_finish(&level);
				return 0;
			}
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
	struct buf level;
	struct cola_elem *p;
	cola_key_t n, l, h, sz;

	dprintf("bsearch level %u (%"PRIu64":%"PRIu64")\n", lvlno, *lo, *hi);
	if ( !read_level_part(c, lvlno, *lo, *hi, &level) )
		return 0;

	sz = *hi - *lo;
	l = 0;
	h = (1U << (lvlno + 1));

	for(*result = 0, p = level.ptr, n = sz; n; ) {
		cola_key_t i = n / 2;
		if ( key < p[i].key ) {
			n = i;
		}else if ( key > p[i].key ) {
			p = p + (i + 1);
			n = n - (i + 1);
		}else{
			*result = 1;
			break;
		}
	}

	if ( *result == 0 ) {
		dprintf(" - nope %"PRIu64" @ %"PRIu64"\n", n, p - level.ptr);
		dprintf(" - lo=%"PRIu64" hi=%"PRIu64"\n", l, h);
	}

	*lo = l;
	*hi = h;
	buf_finish(&level);
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

	printf("%"PRId64" items\n", c->c_nelem);
	for(i = 0; c->c_nelem >= (1U << i); i++) {
		struct buf level;
		unsigned int j;

		if ( !read_level(c, i, &level) )
			return 0;

		if ( !(c->c_nelem & (1U << i)) )
			printf("\033[2;37m");
		printf("level %u:", i);
		for(j = 0; j < (1U << i); j++) {
			printf(" %"PRIu64, level.ptr[j].key);
		}
		if ( !(c->c_nelem & (1U << i)) )
			printf("\033[0m");
		printf("\n");
		buf_finish(&level);
	}

	return 1;
}

int cola_close(cola_t c)
{
	int ret = 1;
	if ( c ) {
		struct cola_hdr *hdr = (struct cola_hdr *)c->c_map;

		if ( c->c_rw ) {
			hdr->h_nelem = c->c_nelem;
			hdr->h_magic = COLA_MAGIC;
			hdr->h_vers = COLA_CURRENT_VER;
			if ( msync(c->c_map, c->c_mapsz, MS_ASYNC) ) {
				ret = 0;
			}
		}
		if ( munmap(c->c_map, c->c_mapsz) ) {
			ret = 0;
		}
		if ( !close(c->c_fd) ) {
			ret = 0;
		}
		free(c);
	}
	return ret;
}
