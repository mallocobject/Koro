#include "koro/hook.h"
#include "elog/logger.h"
#include "koro/channel.h"
#include "koro/context.h"
#include "koro/fiber.h"
#include "koro/file_descriptor.h"
#include "koro/io_manager.h"
#include "koro/scheduler.h"
#include "koro/task.h"
#include <atomic>
#include <cassert>
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

koro::ChannelTable& koro::ctable = koro::Context::channel_table();
koro::IOManager& koro::iom = koro::Context::io_manager();
static std::once_flag flag;

static int (*sys_close)(int) = nullptr;
static ssize_t (*sys_read)(int, void*, size_t) = nullptr;
static ssize_t (*sys_write)(int, const void*, size_t) = nullptr;
static int (*sys_accept)(int, struct sockaddr*, socklen_t*) = nullptr;

static void init_hooks()
{
	sys_close = (int (*)(int))dlsym(RTLD_NEXT, "close");
	sys_read = (ssize_t(*)(int, void*, size_t))dlsym(RTLD_NEXT, "read");
	sys_write = (ssize_t(*)(int, const void*, size_t))dlsym(RTLD_NEXT, "write");
	sys_accept =
		(int (*)(int, struct sockaddr*, socklen_t*))dlsym(RTLD_NEXT, "accept");
}

// Transparent Non-blocking I/O
template <typename SysFunc, typename... Args>
static ssize_t do_io(int fd, SysFunc sys_func, koro::IOManager::Event event,
					 Args&&... args)
{
	auto fiber = koro::Fiber::runningFiber();
	if (!fiber || fiber.get() == koro::t_thread_fiber)
	{
		return sys_func(fd, std::forward<Args>(args)...);
	}

	auto ch = koro::iom.bindTaskQueue(fd);

	if (!ch->sys_non_block_.exchange(true, std::memory_order_acq_rel))
	{
		koro::FD::setNonBlocking(fd, true);
	}

	while (true)
	{
		ssize_t n = sys_func(fd, std::forward<Args>(args)...);
		if (n >= 0 || errno != EAGAIN && errno != EWOULDBLOCK)
		{
			return n;
		}

		if (ch->user_non_block_.load(std::memory_order_acquire))
		{
			return n;
		}

		// save task fiber context
		std::weak_ptr<koro::Fiber> weak_fiber = fiber;

		auto cb = [weak_fiber]()
		{
			if (auto f = weak_fiber.lock())
			{
				std::lock_guard<std::mutex> lock(koro::t_task_queue->mtx);
				koro::t_task_queue->tasks.push_back(
					std::make_shared<koro::ScheduledTask>(f));
			}
		};
		bool ret = koro::iom.registerEvent(ch, event, cb, true);
		if (!ret)
		{
			LOG_WARN << "register event: " << static_cast<uint32_t>(event)
					 << " failed for fd: " << fd;
			return -1;
		}

		fiber->hold(); // change to scheduler fiber

		// return task fiber
		koro::iom.unregisterEvent(ch, event);
	}
}

extern "C"
{
	int close(int fd)
	{
		std::call_once(flag, init_hooks);
		std::shared_ptr<koro::Channel> ch;
		{
			std::lock_guard<std::mutex> lock(koro::ctable.mtx);
			if (fd >= 0 && fd < koro::ctable.chs.size())
			{
				// std::cout << koro::ctable.chs[fd].use_count() << std::endl;
				ch = koro::ctable.chs[fd];
			}
		}

		if (ch)
		{
			koro::iom.removeChannel(ch);
		}

		// std::cout << ch.use_count() << std::endl;

		return sys_close(fd);
	}

	ssize_t read(int fd, void* buf, size_t nbytes)
	{
		std::call_once(flag, init_hooks);
		return do_io(fd, sys_read, koro::IOManager::Event::kRead, buf, nbytes);
	}

	ssize_t write(int fd, const void* buf, size_t n)
	{
		std::call_once(flag, init_hooks);
		const char* ptr = reinterpret_cast<const char*>(buf);
		size_t remain = n;
		ssize_t total = 0;
		while (remain > 0)
		{
			ssize_t ret = do_io(fd, sys_write, koro::IOManager::Event::kWrite,
								ptr, remain);
			if (ret < 0)
			{
				if (total > 0)
				{
					return total;
				}
				return ret;
			}
			if (ret == 0)
			{
				break;
			}
			ptr += ret;
			remain -= ret;
			total += ret;
		}
		return total;
	}

	int accept(int fd, sockaddr* addr, socklen_t* addr_len)
	{
		std::call_once(flag, init_hooks);
		return static_cast<int>(do_io(
			fd, sys_accept, koro::IOManager::Event::kRead, addr, addr_len));
	}
}