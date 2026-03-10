#include "koro/hook.h"
#include "elog/logger.h"
#include "koro/fiber.h"
#include "koro/file_descriptor.h"
#include "koro/io_manager.h"
#include "koro/task.h"
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <dlfcn.h>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <sys/types.h>
#include <unistd.h>
#include <utility>

std::unique_ptr<koro::IOManager> iom = std::make_unique<koro::IOManager>(8);
static std::once_flag flag;

static ssize_t (*sys_read)(int, void*, size_t) = nullptr;
static ssize_t (*sys_write)(int, const void*, size_t) = nullptr;

static void init_hooks()
{
	sys_read = (ssize_t(*)(int, void*, size_t))dlsym(RTLD_NEXT, "read");
	sys_write = (ssize_t(*)(int, const void*, size_t))dlsym(RTLD_NEXT, "write");
}

template <typename SysFunc, typename... Args>
static ssize_t do_io(int fd, SysFunc sys_func, koro::IOManager::Event event,
					 Args&&... args)
{
	auto fiber = koro::Fiber::runningFiber();
	if (!fiber || fiber.get() == koro::t_thread_fiber)
	{
		return sys_func(fd, std::forward<Args>(args)...);
	}

	koro::FD::setNonBlocking(fd);

retry:

	ssize_t n = sys_func(fd, std::forward<Args>(args)...);
	if (n >= 0 || errno != EAGAIN && errno != EWOULDBLOCK)
	{
		return n; // retry 到这就返回，不会陷入轮回
	}

	auto ch = iom->bindTaskQueue(fd);

	// 非对称协程: 永远只能由“调度器”去 resume
	// 其他普通协程。普通协程之间绝对不能互相 resume！
	auto cb = std::make_shared<koro::ScheduledTask>(fiber);
	bool ret = iom->registerEvent(ch, event, cb, true);
	if (!ret)
	{
		LOG_WARN << "register event: " << static_cast<uint32_t>(event)
				 << " failed for fd: " << fd;
	}

	fiber->hold(); // 直接返回
	// 响应后继续执行
	iom->unregisterEvent(ch, event);

	goto retry;
}

extern "C"
{
	ssize_t read(int fd, void* buf, size_t nbytes)
	{
		std::call_once(flag, init_hooks);
		return do_io(fd, sys_read, koro::IOManager::Event::kRead, buf, nbytes);
	}

	ssize_t write(int fd, const void* buf, size_t n)
	{
		std::call_once(flag, init_hooks);
		return do_io(fd, sys_write, koro::IOManager::Event::kWrite, buf, n);
	}
}