#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <cola.h>

static const char *cmd = "cola";

int main(int argc, char **argv)
{
	if ( argc > 0 )
		cmd = argv[0];

	return EXIT_SUCCESS;
}
