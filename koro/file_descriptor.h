#ifndef KORO_FILE_DESCRIPTOR_H
#define KORO_FILE_DESCRIPTOR_H

#include "elog/logger.h"
#include <cstdlib>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#define checkErrno(saved_errno)                                                \
	do                                                                         \
	{                                                                          \
		if ((saved_errno) != 0)                                                \
		{                                                                      \
			::exit(EXIT_FAILURE);                                              \
		}                                                                      \
	} while (0)

namespace koro
{
namespace FD
{
inline int createEventFd()
{
	int fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (fd == -1)
	{
		LOG_FATAL << "create event fd failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}
	return fd;
}

inline int createEpollFd()
{
	int fd = ::epoll_create1(0);
	if (fd == -1)
	{
		LOG_FATAL << "create epoll fd failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}
	return fd;
}

inline int createTimerFd()
{
	int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
	if (fd == -1)
	{
		LOG_FATAL << "create timer fd failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}
	return fd;
}

inline void close(int& fd)
{
	if (fd != -1)
	{
		::close(fd);
		fd = -1;
	}
}

inline bool setNonBlocking(int fd, bool on = true)
{
	int flags = ::fcntl(fd, F_GETFL, 0);
	if (flags == -1)
	{
		int err = errno;
		LOG_ERROR << "fcntl(F_GETFL) failed for fd " << fd << ": "
				  << ::strerror(err);
		return false;
	}

	int new_flags = on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);

	if (new_flags == flags)
	{
		return true;
	}

	if (::fcntl(fd, F_SETFL, new_flags) == -1)
	{
		int err = errno;
		LOG_ERROR << "fcntl(F_SETFL) failed for fd " << fd << ": "
				  << ::strerror(err);
		return false;
	}

	return true;
}
} // namespace FD
} // namespace koro

#endif