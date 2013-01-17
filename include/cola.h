/*
* This file is part of cola
* Copyright (c) 2013 Gianni Tedesco
* This program is released under the terms of the GNU GPL version 2
*/
#ifndef _COLA_H
#define _COLA_H

#include "cola-common.h"

typedef struct _cola *cola_t;
extern const char *cmd;

cola_t cola_open(const char *fn, int rw);
cola_t cola_creat(const char *fn, int overwrite); /* always rw */
int cola_insert(cola_t c, cola_key_t key);
int cola_query(cola_t c, cola_key_t key, int *result);
int cola_dump(cola_t c);
int cola_close(cola_t c);

#endif /* _COLA_H */
