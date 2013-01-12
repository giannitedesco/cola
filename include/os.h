/*
 * This file is part of dotscara
 * Copyright (c) 2003 Gianni Tedesco
 * Released under the terms of the GNU GPL version 2
 */
#ifndef _FIRESTORM_OS_HEADER_INCLUDED_
#define _FIRESTORM_OS_HEADER_INCLUDED_

const uint8_t *map_file(int fd, size_t *len);
int os_errno(void);
const char *os_error(int);
const char *os_err(void);
const char *os_err2(const char *);

int fd_read(int fd, void *buf, size_t *sz, int *eof) _check_result;
int fd_pread(int fd, off_t off, void *buf, size_t *sz, int *eof) _check_result;
int fd_write(int fd, const void *buf, size_t len) _check_result;
int fd_pwrite(int fd, off_t off, const void *buf, size_t len) _check_result;
int fd_close(int fd);

int fd_block(int fd, int b);
int fd_coe(int fd, int coe);

int os_sigpipe_ignore(void);

#endif /* _FIRESTORM_OS_HEADER_INCLUDED_ */
