#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>

#include <cola.h>
#include <cola-format.h>
#include <os.h>

#if 0
#define dprintf printf
#else
#define dprintf(x...) do {} while(0)
#endif

struct _cola {
	int c_fd;
	cola_key_t c_nelem;
};

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
		hdr.h_nelem = 0;
		hdr.h_magic = COLA_MAGIC;
		hdr.h_vers = COLA_CURRENT_VER;
		if ( !fd_write(c->c_fd, &hdr, sizeof(hdr)) ) {
			fprintf(stderr, "%s: write: %s: %s\n",
				cmd, fn, os_err());
			goto out_close;
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

static struct cola_elem *read_level(struct _cola *c, unsigned int lvlno)
{
	struct cola_elem *level = NULL;
	cola_key_t nr_ent, ofs;
	size_t sz;
	int eof;

	nr_ent = (1 << lvlno);
	ofs = nr_ent - 1;

	sz = nr_ent * sizeof(*level);
	level = malloc(sz);
	if ( NULL == level )
		goto out;

	ofs *= sizeof(*level);
	ofs += sizeof(struct cola_hdr);

	if ( !fd_pread(c->c_fd, ofs, level, &sz, &eof) ||
			sz != (nr_ent * sizeof(*level)) ) {
		fprintf(stderr, "%s: read: %s\n",
			cmd, os_err2("File truncated"));
		goto out_free;
	}

	goto out; /* success */

out_free:
	free(level);
	level = NULL;
out:
	return level;
}

static int write_level(struct _cola *c, unsigned int lvlno,
			struct cola_elem *level)
{
	cola_key_t nr_ent, ofs;
	size_t sz;

	nr_ent = (1 << lvlno);
	sz = nr_ent * sizeof(*level);
	ofs = nr_ent - 1;
	ofs *= sizeof(*level);
	ofs += sizeof(struct cola_hdr);

	return fd_pwrite(c->c_fd, ofs, level, sz);
}

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
			printf(" - dupe key %"PRIu64"\n", b[bb].key);
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

	return m;
}

int cola_insert(cola_t c, cola_key_t key)
{
	cola_key_t newcnt = c->c_nelem + 1;
	struct cola_elem *level;
	unsigned int i;

	level = malloc(sizeof(*level));
	if ( NULL == level )
		return 0;

	level->key = key;

	for(i = 0; newcnt >= (1U << i); i++) {
		if ( c->c_nelem & (1U << i) ) {
			struct cola_elem *level2, *merged;
			printf(" - level %u full\n", i);
			level2 = read_level(c, i);
			if ( NULL == level2 ) {
				free(level);
				return 0;
			}
			merged = level_merge(level, level2, i);
			free(level2);
			free(level);
			level = merged;
			if ( NULL == merged )
				return 0;
		}else{
			printf(" - level %u empty\n", i);
			if ( !write_level(c, i, level) ) {
				free(level);
				return 0;
			}
			break;
		}
	}

	c->c_nelem = newcnt;
	return 1;
}

static int query_level(struct _cola *c, cola_key_t key,
			unsigned int lvlno, int *result)
{
	struct cola_elem *level;
	unsigned int i;

	level = read_level(c, lvlno);
	if ( NULL == level )
		return 0;

	/* FIXME: bsearch */
	for(i = 0; i < (1U << lvlno); i++) {
		if ( level[i].key == key ) {
			*result = 1;
			break;
		}
	}

	free(level);
	return 1;
}

int cola_query(cola_t c, cola_key_t key, int *result)
{
	unsigned int i;

	for(i = 0; c->c_nelem >= (1U << i); i++) {
		if ( !(c->c_nelem & (1 << i)) )
			continue;
		dprintf("bsearch level %u\n", i);
		if ( !query_level(c, key, i, result) )
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
	if ( c ) {
		struct cola_hdr hdr;
		hdr.h_nelem = c->c_nelem;
		hdr.h_magic = COLA_MAGIC;
		hdr.h_vers = COLA_CURRENT_VER;
		if ( !fd_pwrite(c->c_fd, 0, &hdr, sizeof(hdr)) )
			return 0;
		if ( !fd_close(c->c_fd) )
			return 0;
		free(c);
	}
	return 1;
}
