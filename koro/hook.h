#ifndef KORO_HOOK_H
#define KORO_HOOK_H

#include "koro/channel.h"
#include "koro/io_manager.h"
#include <sys/socket.h>
#include <unistd.h>
#ifdef __cplusplus
extern "C"
{
#endif
	static_assert(sizeof(koro::ChannelTable));
	static_assert(sizeof(koro::IOManager));

	int close(int fd);
	ssize_t read(int fd, void* buf, size_t nbytes);
	ssize_t write(int fd, const void* buf, size_t n);
	int accept(int fd, sockaddr* addr, socklen_t* addr_len);

#ifdef __cplusplus
}
#endif

#endif