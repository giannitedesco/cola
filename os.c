/*
* This file is part of Firestorm NIDS
* Copyright (c) 2003 Gianni Tedesco
* Released under the terms of the GNU GPL version 2
*/

#include <errno.h>
#include <stdint.h>
#include <string.h>
#define __USE_UNIX98
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <stdio.h>

#include <compiler.h>
#include <os.h>

const uint8_t *map_file(int fd, size_t *len)
{
	struct stat st;
	const uint8_t *map;

	*len = 0;

	if ( fstat(fd, &st) )
		return NULL;

	map = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if ( map == MAP_FAILED )
		return NULL;

	*len = st.st_size;
	return map;
}

int os_errno(void)
{
	return errno;
}

const char *os_error(int e)
{
	return strerror(e);
}

const char *os_err(void)
{
	return strerror(errno);
}

const char *os_err2(const char *def)
{
	if ( def == NULL )
		def = "Internal Error";
	return (errno ? strerror(errno) : def);
}
/*
 * \ingroup g_fdctl
 * @param fd: File descriptor to wait for events on
 * @param flags: Poll events to wait for
 *
 * Waits for events on a non-blocking file descriptor. This is an internal
 * API used by fibuf/fobuf etc. if the fd they are trying to use returns
 * EAGAIN, this returns the blocking behaivour.
 *
 * Return value: 1 on success, 0 on error.
 */
static int fd_wait_single(int fd, int flags)
{
	struct pollfd pfd;
	int ret;

	/* Some systems don't indicate POLLIN on EOF, rather they
	 * use POLLHUP, we always check for POLLHUP then use read/
	 * write to see what the real problem is.
	 */
	flags |= POLLHUP;

	pfd.fd = fd;
	pfd.events = flags | POLLERR;
	pfd.revents = 0;

again:
	ret = poll(&pfd, 1, -1);
	if ( ret < 0 ) {
		if ( errno == EINTR )
			goto again;
		return 0;
	}

	if ( pfd.revents & flags )
		return 1;

	/* POLLERR */
	/* XXX: we return 1 here and force the caller
	 * to attempt an I/O operation in order to make sure that
	 * the correct value ends up in errno
	 */
	return 1;
}

/** Close a file descriptor.
 * \ingroup g_fdctl
 * @param fd The file descriptor to close.
 *
 * Closes a file descriptor handling all possible errors. I bet you didn't
 * know that close(2) could return EINTR.
 *
 * @return 0 on error, 1 on success.
*/

int fd_close(int fd)
{
	int ret;
intr:
	ret = close(fd);
	if ( ret && errno == EINTR )
		goto intr;

	return (ret == 0);
}

/** Configure blocking mode on a file descriptor.
 * \ingroup g_fdctl
 * @param fd FD to set blocking mode on
 * @param b Whether to enable or disable blocking mode
 *
 * Configures blocking mode on a file descriptor.
 *
 * @return 0 on error, 1 on success.
 */
int fd_block(int fd, int b)
{
	int fl;

	fl = fcntl(fd, F_GETFL);
	if ( fl < 0 )
		return 0;

	if ( b )
		fl &= ~O_NONBLOCK;
	else
		fl |= O_NONBLOCK;

	fl = fcntl(fd, F_SETFL, fl);
	if ( fl < 0 )
		return 0;

	return 1;
}

/** Configure close-on-exec mode for a file descriptor.
 * \ingroup g_fdctl
 * @param fd FD upon which to configure close-on-exec mode
 * @param coe Whether to enable or disable close-on-exec
 *
 * Allows the fd's close-on-exec flag to be configured.
 *
 * @return 0 on error, 1 on success.
 */
int fd_coe(int fd, int coe)
{
	int ret;

	if ( coe )
		coe = FD_CLOEXEC;
	else
		coe = 0;

	ret = fcntl(fd, F_SETFD, coe);
	if ( ret < 0 )
		return 0;

	return 1;
}

/** Read from a file descriptor handling all errors.
 * \ingroup g_fdctl
 * @param fd file descriptor
 * @param buf data to read
 * @param sz pointer to size of buffer
 * @param eof EOF flag, is set to 1 if EOF is hit, may not be NULL
 *
 * Call read(2) with given parameters but handle all possible
 * errors. We handle short reads, interrupted calls, fd going
 * O_NONBLOCK under us, and only bail on really unrecoverable
 * errors.
 *
 * @return 0 on unrecoverable error, 1 on success. Even an
 * unrecoverable error may return a non-zero value in sz and
 * hence valid data. If EOF is experienced on the file the
 * eof flag is set to 1.
 */
int fd_read(int fd, void *buf, size_t *sz, int *eof)
{
	size_t len = *sz;
	ssize_t ret;

	*sz = 0;

again:
	ret = read(fd, buf, likely(len < SSIZE_MAX) ? len : SSIZE_MAX);
	if ( ret < 0 ) {
		if ( errno == EINTR )
			goto again;
		if ( errno == EAGAIN &&
			fd_wait_single(fd, POLLIN) )
			goto again;
		return 0;
	}

	if ( (size_t)ret == 0 ) {
		*eof = 1;
		return 1;
	}

	*sz += (size_t)ret;

	/* This can happen on a regular file if a long I/O is interrupted
	 * for example on NFS in soft/interruptable mode in the Linux kernel.
	 * It can also happen on sockets and character devices.
	 */
	if ( (size_t)ret < len ) {
		buf += (size_t)ret;
		len -= (size_t)ret;
		goto again;
	}

	return 1;
}

/** Read from a file descriptor handling all errors.
 * \ingroup g_fdctl
 * @param fd file descriptor
 * @param buf data to read
 * @param sz pointer to size of buffer
 * @param eof EOF flag, is set to 1 if EOF is hit, may not be NULL
 *
 * Call read(2) with given parameters but handle all possible
 * errors. We handle short reads, interrupted calls, fd going
 * O_NONBLOCK under us, and only bail on really unrecoverable
 * errors.
 *
 * @return 0 on unrecoverable error, 1 on success. Even an
 * unrecoverable error may return a non-zero value in sz and
 * hence valid data. If EOF is experienced on the file the
 * eof flag is set to 1.
 */
int fd_pread(int fd, off_t off, void *buf, size_t *sz, int *eof)
{
	size_t len = *sz;
	ssize_t ret;

	*sz = 0;

again:
	ret = pread(fd, buf, likely(len < SSIZE_MAX) ? len : SSIZE_MAX, off);
	if ( ret < 0 ) {
		if ( errno == EINTR )
			goto again;
		if ( errno == EAGAIN &&
			fd_wait_single(fd, POLLIN) )
			goto again;
		return 0;
	}

	if ( (size_t)ret == 0 ) {
		*eof = 1;
		return 1;
	}

	*sz += (size_t)ret;

	/* This can happen on a regular file if a long I/O is interrupted
	 * for example on NFS in soft/interruptable mode in the Linux kernel.
	 * It can also happen on sockets and character devices.
	 */
	if ( (size_t)ret < len ) {
		off += (off_t)ret;
		buf += (size_t)ret;
		len -= (size_t)ret;
		goto again;
	}

	return 1;
}

/** Write to a file descriptor handling all errors.
 * \ingroup g_fdctl
 * @param fd file descriptor
 * @param buf data to write
 * @param len length of data
 *
 * Call write(2) with given parameters but handle all possible
 * errors. We handle short writes, interrupted calls, fd going
 * O_NONBLOCK under us, and only bail on really unrecoverable
 * errors.
 *
 * Failure Modes:
 *  -# undefined: any failure mode of fd_wait_single()
 *  -# undefined: any kernel failure mode
 * @return 0 on unrecoverable error, 1 on success.
 */
int fd_write(int fd, const void *buf, size_t len)
{
	ssize_t ret;

again:
	ret = write(fd, buf, likely(len < SSIZE_MAX) ? len : SSIZE_MAX);
	if ( ret < 0 ) {
		if ( errno == EINTR )
			goto again;
		if ( errno == EAGAIN &&
			fd_wait_single(fd, POLLOUT) )
			goto again;
		return 0;
	}

	/* This can happen on a regular file if a long I/O is interrupted
	 * for example on NFS in soft/interruptable mode in the Linux kernel.
	 * It can also happen on sockets and character devices.
	 */
	if ( (size_t)ret < len ) {
		buf += (size_t)ret;
		len -= (size_t)ret;
		goto again;
	}

	return 1;
}

int os_sigpipe_ignore(void)
{
	if ( signal(SIGPIPE, SIG_IGN) ) {
		fprintf(stderr, "signal: %s\n", os_err());
		return 0;
	}

	return 1;
}
