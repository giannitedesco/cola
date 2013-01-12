#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

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
		if ( !fd_read(c->c_fd, &hdr, &sz, &eof) ||
				eof || sz != sizeof(hdr) ) {
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

int cola_insert(cola_t c, cola_key_t key)
{
	return 0;
}

static int query_level(struct _cola *c, cola_key_t key,
			unsigned int lvlno, int *result)
{
	struct cola_elem *level;
	cola_key_t nr_ent, ofs;
	unsigned int i;
	size_t sz;
	int eof, ret = 0;

	nr_ent = (1 << lvlno);
	ofs = nr_ent - 1;

	printf(" - query level %u (%"PRIu64" keys @ %"PRIu64")\n",
		lvlno , nr_ent, ofs);

	sz = nr_ent * sizeof(*level);
	level = malloc(sz);
	if ( NULL == level )
		goto out;

	ofs *= sizeof(*level);
	ofs += sizeof(struct cola_hdr);

	if ( !fd_read(c->c_fd, level, &sz, &eof) ||
			eof || sz != (nr_ent * sizeof(*level)) ) {
		fprintf(stderr, "%s: read: %s\n",
			cmd, os_err2("File truncated"));
		goto out_free;
	}

	ret = 1;

	/* FIXME: bsearch */
	for(i = 0; i < nr_ent; i++) {
		if ( level[i].key == key ) {
			*result = 1;
			break;
		}
	}

out_free:
	free(level);
out:
	return ret;
}

int cola_query(cola_t c, cola_key_t key, int *result)
{
	unsigned int i;

	for(i = 0; c->c_nelem & (1 << i); i++) {
		dprintf("bsearch level %u\n", i);
		if ( !query_level(c, key, i, result) )
			return 0;
		if ( *result )
			return 1;
	}

	*result = 0;
	return 1;
}

void cola_close(cola_t c)
{
	if ( c ) {
		fd_close(c->c_fd);
		free(c);
	}
}
