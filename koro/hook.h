#ifndef KORO_HOOK_H
#define KORO_HOOK_H

#include "koro/io_manager.h"
#include <unistd.h>
#ifdef __cplusplus
extern "C"
{
#endif

	extern std::unique_ptr<koro::IOManager> iom;
	ssize_t read(int fd, void* buf, size_t nbytes);
	ssize_t write(int fd, const void* buf, size_t n);

#ifdef __cplusplus
}
#endif

#endif