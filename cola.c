#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <cola.h>

const char *cmd = "cola";

static int usage(int code)
{
	FILE *f = (code) ? stderr : stdout;

	fprintf(f, "%s: Usage\n", cmd);
	fprintf(f, "\t$ %s create [-f] <fn>\n", cmd);
	fprintf(f, "\t$ %s query <fn> <key>\n", cmd);
	fprintf(f, "\t$ %s insert <fn> <key>\n", cmd);
	fprintf(f, "\t$ %s dump <fn>\n", cmd);
	fprintf(f, "\t$ %s help\n", cmd);
	fprintf(f, "\n");

	return code;
}

static int do_insertrandom(int argc, char **argv)
{
	const char *fn;
	cola_key_t seed, count, i;
	cola_t c;

	if ( argc < 4 )
		return usage(EXIT_FAILURE);

	fn = argv[1];
	if ( !cola_parse_key(argv[2], &seed) )
		return usage(EXIT_FAILURE);
	if ( !cola_parse_key(argv[3], &count) )
		return usage(EXIT_FAILURE);

	c = cola_open(fn, 1);
	if ( NULL == c )
		return EXIT_FAILURE;

	srand(seed);
	for(i = 0; i < count; i++) {
		//if ( !cola_insert(c, rand()) ) {
		if ( !cola_insert(c, i) ) {
			cola_close(c);
			return EXIT_FAILURE;
		}
	}

	cola_close(c);
	return EXIT_SUCCESS;
}

static int do_create(int argc, char **argv)
{
	struct _cola *c;
	const char *fn;
	int force = 0;
	int i = 1;

	switch(argc) {
	case 3:
		if ( !strcmp(argv[1], "-f") ) {
			force = 1;
			i++;
		}else
			return usage(EXIT_FAILURE);
	case 2:
		fn = argv[i];
		break;
	case 1:
	default:
		return usage(EXIT_FAILURE);
	}

	c = cola_creat(fn, force);
	if ( NULL == c ) {
		cola_close(c);
		return EXIT_FAILURE;
	}

	cola_close(c);
	return EXIT_SUCCESS;
}

static int do_insert(int argc, char **argv)
{
	const char *fn;
	cola_key_t key;
	cola_t c;

	if ( argc < 3 )
		return usage(EXIT_FAILURE);

	fn = argv[1];
	if ( !cola_parse_key(argv[2], &key) )
		return usage(EXIT_FAILURE);

	c = cola_open(fn, 1);
	if ( NULL == c )
		return EXIT_FAILURE;

	if ( !cola_insert(c, key) ) {
		cola_close(c);
		return EXIT_FAILURE;
	}

	cola_close(c);
	return EXIT_SUCCESS;
}

static int do_query(int argc, char **argv)
{
	const char *fn;
	cola_key_t key;
	int result;
	cola_t c;

	if ( argc < 3 )
		return usage(EXIT_FAILURE);

	fn = argv[1];
	if ( !cola_parse_key(argv[2], &key) )
		return usage(EXIT_FAILURE);

	c = cola_open(fn, 0);
	if ( NULL == c )
		return EXIT_FAILURE;

	if ( !cola_query(c, key, &result) ) {
		cola_close(c);
		return EXIT_FAILURE;
	}

	printf("key %"PRId64" %sfound\n",
		key, (result) ? "" : "not ");

	cola_close(c);
	return EXIT_SUCCESS;
}

static int do_dump(int argc, char **argv)
{
	const char *fn;
	cola_t c;

	if ( argc < 2 )
		return usage(EXIT_FAILURE);

	fn = argv[1];
	c = cola_open(fn, 0);
	if ( NULL == c )
		return EXIT_FAILURE;

	if ( !cola_dump(c) ) {
		cola_close(c);
		return EXIT_FAILURE;
	}

	cola_close(c);
	return EXIT_SUCCESS;
}

int main(int argc, char **argv)
{
	unsigned int i;
	static const struct {
		const char * const cmd;
		int (*fn)(int argc, char **argv);
	}fn[] = {
		{"create", do_create},
		{"query", do_query},
		{"insert", do_insert},
		{"insertrandom", do_insertrandom},
		{"dump", do_dump},
	};

	if ( argc > 0 )
		cmd = argv[0];
	if ( argc < 2 )
		return usage(EXIT_FAILURE);

	for(i = 0; i < sizeof(fn)/sizeof(*fn); i++) {
		if ( !strcmp(fn[i].cmd, argv[1]) ) {
			return fn[i].fn(argc - 1, argv + 1);
		}
	}

	return usage(EXIT_FAILURE);
}
