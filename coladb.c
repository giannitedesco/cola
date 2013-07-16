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
#include <minheap.h>
#include <os.h>

#define INITIAL_LEVELS		17 /* 128K */
#define MAP_LEVELS		23 /* 8M */

//#define DEBUG 1
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

struct inbuf {
	int mapped;
	union {
		struct {
			struct cola_elem *buf;
			struct cola_elem *end;
		}mapped;
		struct {
			struct cola_elem *buf;
			struct cola_elem *cur;
			struct cola_elem *end;
			cola_key_t off;
		}buffered;
	}u;
};

struct outbuf {
	union {
		struct {
			struct cola_elem *ptr;
			struct cola_elem *end;
		}mapped;
		struct {
			struct cola_elem *cur;
			struct cola_elem *end;
			unsigned int lvlno;
		}buffered;
	}u;
	int mapped;
};

static cola_key_t level_ofs(unsigned int lvlno)
{
	cola_key_t ofs;

	ofs = (1U << lvlno) - 1;
	ofs *= sizeof(struct cola_elem);
	ofs += sizeof(struct cola_hdr);

	return ofs;
}

static void outbuf_init(struct outbuf *out, struct _cola *c, unsigned int lvlno)
{
	if ( lvlno <= c->c_maplvls ) {
		out->u.mapped.ptr = (struct cola_elem *)(c->c_map +
							level_ofs(lvlno));
		out->u.mapped.end = out->u.mapped.ptr + (1U << lvlno);
		out->mapped = 1;
	}else{
		assert(lvlno < c->c_maplvls);
		out->mapped = 0;
	}
}

static void outbuf_push(struct outbuf *out, struct cola_elem *e)
{
	if ( out->mapped ) {
		assert(out->u.mapped.ptr < out->u.mapped.end);
		out->u.mapped.ptr[0] = *e;
		out->u.mapped.ptr++;
	}else{
		assert(out->mapped);
	}
}

static void inbuf_one_item(struct inbuf *in, struct cola_elem *one)
{
	in->mapped = 1;
	in->u.mapped.buf = one;
	in->u.mapped.end = one + 1;
}

static void inbuf_init(struct inbuf *in, struct _cola *c, unsigned int lvlno)
{
	if ( lvlno <= c->c_maplvls ) {
		in->mapped = 1;
		in->u.mapped.buf = (struct cola_elem *)(c->c_map +
							level_ofs(lvlno));
		in->u.mapped.end = in->u.mapped.buf + (1U << lvlno);
	}else{
		assert(lvlno < c->c_maplvls);
		in->mapped = 0;
	}
}

static int inbuf_pop(struct inbuf *in, cola_key_t *ret)
{
	if ( in->mapped ) {
		if ( in->u.mapped.buf >= in->u.mapped.end )
			return 0;
		*ret = in->u.mapped.buf[0].key;
		in->u.mapped.buf++;
		return 1;
	}else{
		assert(in->mapped);
	}
	return 0;
}

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

static int read_level_part(struct _cola *c, unsigned int lvlno,
					cola_key_t from, cola_key_t to,
					struct buf *buf)
{
	cola_key_t nr_ent, ofs;
	int eof;

	assert(from <= to);
	assert(to <= (1U << lvlno));

	nr_ent = to - from;
	ofs = level_ofs(lvlno);
	ofs += from;

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

int cola_insert(cola_t c, cola_key_t key)
{
	cola_key_t newcnt = c->c_nelem + 1;
	struct inbuf *in;
	struct outbuf out;
	struct heap_item *h;
	unsigned int k, i, outlvl;
	struct cola_elem elem;

	elem.key = key;

	dprintf("Insert key %"PRIu64"\n", key);

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

	outlvl = __builtin_ctzl(~c->c_nelem & newcnt);
	k = outlvl + 1;
	dprintf(" - will write to level %u (%u-way merge)\n",
			outlvl, k);
	h = alloca(sizeof(*h) * k);
	if ( NULL == h )
		return 0;

	in = alloca(sizeof(*in) * k);
	if ( NULL == in )
		return 0;

	for(i = 0; i < k; i++) {
		if ( i == 0 ) {
			inbuf_one_item(in + i, &elem);
		}else{
			inbuf_init(in + i, c, i - 1);
		}
	}

	/* initialize the heap */
	h = h - 1;
	for(i = 1; i <= k; i++) {
		h[i].val = i - 1;
		inbuf_pop(in + h[i].val, &h[i].key);
	}
	minheap_init(k, h);

	/* k-way merge in to output buffer */
	outbuf_init(&out, c, outlvl);
	//for(i = 0; i < (1U << outlvl); i++) {
	while(k) {
		struct cola_elem oelem;
		cola_key_t next;
		unsigned long next_in;

		next_in = h[1].val;
		oelem.key = h[1].key;

		outbuf_push(&out, &oelem);

		/* delete item from heap */
		h[1] = h[k];
		minheap_sift_down(k - 1, h);

		if ( inbuf_pop(&in[next_in], &next) ) {
			/* re-add to heap */
			h[k].key = next;
			h[k].val = next_in;
			minheap_sift_up(k, h);
		}else
			k--;
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
