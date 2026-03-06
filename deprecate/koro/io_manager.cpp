#include "koro/io_manager.h"
#include "elog/logger.h"
#include "koro/current_thread.h"
#include "koro/fiber.h"
#include "koro/scheduler.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <utility>

using namespace koro;

IOManager::FdContext::EventContext& IOManager::FdContext::eventContext(
	Event event)
{
	assert(event == Event::kRead || event == Event::kWrite);
	switch (event)
	{
	case Event::kRead:
		return read_ctx;
	case Event::kWrite:
		return write_ctx;
	default:
		LOG_FATAL << "Unsupported event type: " << static_cast<uint8_t>(event);
		throw std::invalid_argument("Unsupported event type");
	}
}

void IOManager::FdContext::resetEventContext(EventContext& event_ctx)
{
	event_ctx.scheduler = nullptr;
	event_ctx.fiber.reset();
	event_ctx.cb = nullptr;
}

void IOManager::FdContext::triggerEvent(Event event)
{
	if (!(events & event))
	{
		return;
	}

	events = events & (~event);

	EventContext& event_ctx = eventContext(event);
	assert(event_ctx.scheduler);
	if (event_ctx.cb)
	{
		event_ctx.scheduler->submit(std::move(event_ctx.cb));
	}
	else
	{
		event_ctx.scheduler->submit(event_ctx.fiber);
	}

	resetEventContext(event_ctx);
}

IOManager::IOManager(size_t thread_count) : Scheduler(thread_count)
{
	epfd_ = ::epoll_create1(0);
	if (epfd_ < 0)
	{
		LOG_FATAL << "create epoll failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}

	wakeup_fds_.resize(thread_count, -1);
	for (size_t i = 0; i < wakeup_fds_.size(); i++)
	{
		wakeup_fds_[i] = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
		if (wakeup_fds_[i] == -1)
		{
			LOG_FATAL << "create eventfd failed for thread: " << i << ": "
					  << ::strerror(errno);
			::exit(EXIT_FAILURE);
		}
		epoll_event ev;
		ev.events = EPOLLIN | EPOLLET;
		ev.data.fd = wakeup_fds_[i];

		if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, wakeup_fds_[i], &ev))
		{
			LOG_FATAL << "add wakeup fd " << wakeup_fds_[i]
					  << " to epoll failed: " << ::strerror(errno);
			::exit(EXIT_FAILURE);
		}
	}

	init();
}

IOManager::~IOManager()
{
	stop();

	if (epfd_ > 0)
	{
		::close(epfd_);
		epfd_ = -1;
	}

	for (int wakeup_fd : wakeup_fds_)
	{
		if (wakeup_fd > 0)
		{
			::close(wakeup_fd);
			wakeup_fd = -1;
		}
	}
	wakeup_fds_.clear();

	std::lock_guard<std::mutex> lock(mtx_);
	for (auto& item : fd_ctxes_)
	{
		if (item.second)
		{
			delete item.second;
		}
	}
	fd_ctxes_.clear();
}

IOManager* IOManager::localStance()
{
	return dynamic_cast<IOManager*>(Scheduler::localStance());
}

void IOManager::stop()
{
	if (stopping())
	{
		return;
	}

	stop_.store(true, std::memory_order_release);

	for (size_t i = 0; i < wakeup_fds_.size(); i++) // 唤醒 epoll_wait 的线程
	{
		wakeupThread(i);
	}

	while (pending_event_count.load(std::memory_order_acquire) > 0)
	{
		std::this_thread::yield();
	}

	stop_.store(false, std::memory_order_release);

	Scheduler::stop();
}

bool IOManager::addEvent(int fd, Event event, function cb)
{
	if (fd < 0 || (event != Event::kRead && event != Event::kWrite))
	{
		LOG_ERROR << "invalid fd or event: fd=" << fd
				  << ", event=" << static_cast<int>(event);
		return false;
	}

	{
		std::lock_guard<std::mutex> lock(mtx_);
		FdContext* fd_ctx = nullptr;
		auto it = fd_ctxes_.find(fd);
		if (it == fd_ctxes_.end())
		{
			fd_ctx = new FdContext;
			fd_ctx->fd = fd;
			fd_ctxes_[fd] = fd_ctx;
		}
		else
		{
			fd_ctx = it->second;
		}

		if (!!(fd_ctx->events & event))
		{
			LOG_WARN << "event already exists: fd=" << fd
					 << ", event=" << static_cast<uint8_t>(event);
			return false;
		}

		int op = !!(fd_ctx->events) ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
		epoll_event ev;
		ev.events = EPOLLET | static_cast<uint32_t>(fd_ctx->events | event);
		ev.data.ptr = fd_ctx;

		if (::epoll_ctl(epfd_, op, fd, &ev))
		{
			LOG_ERROR << "epoll_ctl failed: fd=" << fd << ", op=" << op
					  << ", err=" << ::strerror(errno);
			return false;
		}

		pending_event_count.fetch_add(1, std::memory_order_relaxed);
		fd_ctx->events = fd_ctx->events | event;

		FdContext::EventContext& event_ctx = fd_ctx->eventContext(event);
		assert(!event_ctx.scheduler && !event_ctx.cb && !event_ctx.fiber);

		event_ctx.scheduler = Scheduler::localStance();
		if (cb)
		{
			event_ctx.cb = std::move(cb);
		}
		else
		{
			event_ctx.fiber = Fiber::curFiberPtr(); // 确保在主协程
			assert(event_ctx.fiber &&
				   event_ctx.fiber->state() == Fiber::State::kRunning);
		}
	}
	if (!cb)
	{
		Fiber::curFiberPtr()->yield();
	}
	return true;
}

bool IOManager::cancelEvent(int fd, Event event)
{
	if (fd < 0 || (event != Event::kRead && event != Event::kWrite))
	{
		LOG_ERROR << "invalid fd or event: fd=" << fd
				  << ", event=" << static_cast<int>(event);
		return false;
	}

	std::lock_guard<std::mutex> lock(mtx_);

	auto it = fd_ctxes_.find(fd);
	if (it == fd_ctxes_.end())
	{
		LOG_WARN << "fd not found: " << fd;
		return false;
	}

	FdContext* fd_ctx = it->second;
	if (!(fd_ctx->events & event))
	{
		LOG_WARN << "event not found: fd=" << fd
				 << ", event=" << static_cast<uint8_t>(event);
		return false;
	}

	Event new_events = fd_ctx->events & ~event;

	if (!cancelEventImpl(fd_ctx, new_events))
	{
		return false;
	}

	FdContext::EventContext& event_ctx = fd_ctx->eventContext(event);
	FdContext::resetEventContext(event_ctx);

	pending_event_count.fetch_sub(1, std::memory_order_relaxed);
	fd_ctx->events = new_events;

	if (!new_events)
	{
		fd_ctxes_.erase(it);
		delete fd_ctx;
	}

	return true;
}

// 取消IO事件（等待执行完成后取消）
bool IOManager::cancelEventAfterDone(int fd, Event event)
{
	if (fd < 0 || (event != Event::kRead && event != Event::kWrite))
	{
		LOG_ERROR << "invalid fd or event: fd=" << fd
				  << ", event=" << static_cast<int>(event);
		return false;
	}

	std::lock_guard<std::mutex> lock(mtx_);

	auto it = fd_ctxes_.find(fd);
	if (it == fd_ctxes_.end())
	{
		LOG_WARN << "fd not found: " << fd;
		return false;
	}

	FdContext* fd_ctx = it->second;
	if (!(fd_ctx->events & event))
	{
		LOG_WARN << "event not found: fd=" << fd
				 << ", event=" << static_cast<int>(event);
		return false;
	}

	Event new_events = fd_ctx->events & ~event;

	if (!cancelEventImpl(fd_ctx, new_events))
	{
		return false;
	}

	// fd_ctx->events = new_events;
	fd_ctx->triggerEvent(event); // tiggerEvent 里重新设置events
	pending_event_count.fetch_sub(1, std::memory_order_relaxed);

	if (!new_events)
	{
		fd_ctxes_.erase(it);
		delete fd_ctx;
	}

	return true;
}

// 取消fd的所有事件（等待执行完成）
bool IOManager::cancelEventAfterDone(int fd)
{
	if (fd < 0)
	{
		LOG_ERROR << "invalid fd: " << fd;
		return false;
	}

	std::lock_guard<std::mutex> lock(mtx_);

	auto it = fd_ctxes_.find(fd);
	if (it == fd_ctxes_.end())
	{
		LOG_WARN << "fd not found: " << fd;
		return false;
	}

	FdContext* fd_ctx = it->second;
	if (fd_ctx->events == Event::kNone)
	{
		fd_ctxes_.erase(it);
		delete fd_ctx;
		return true;
	}

	if (!cancelEventImpl(fd_ctx, Event::kNone))
	{
		return false;
	}

	size_t count = 0;
	if (!!(fd_ctx->events & Event::kRead))
	{
		fd_ctx->triggerEvent(Event::kRead);
		count++;
	}
	if (!!(fd_ctx->events & Event::kWrite))
	{
		fd_ctx->triggerEvent(Event::kWrite);
		count++;
	}

	pending_event_count.fetch_sub(count, std::memory_order_relaxed);

	fd_ctxes_.erase(it);
	delete fd_ctx;
	return true;
}

void IOManager::tickle()
{
	if (!hasIdleThread())
	{
		return;
	}

	static std::atomic<size_t> next_wakeup_idx{0};
	size_t idx = next_wakeup_idx.fetch_add(1, std::memory_order_relaxed) %
				 wakeup_fds_.size();
	int wakeup_fd = wakeup_fds_[idx];

	if (wakeup_fd < 0)
	{
		LOG_WARN << "invalid wakeup fd at idx " << idx << ": " << wakeup_fd;
		return;
	}

	wakeupThread(idx);
}

void IOManager::idle()
{
	static const size_t MAX_EVENTS = 1024;
	std::unique_ptr<epoll_event[]> evs =
		std::make_unique<epoll_event[]>(MAX_EVENTS);

	while (true)
	{
		LOG_DEBUG << "idle fiber runs in thread: " << CurrentThread::tid();

		if (stopping())
		{
			LOG_DEBUG << "idle fiber exits in thread: " << CurrentThread::tid();
			break;
		}

		// 超时100ms
		int nevs = ::epoll_wait(epfd_, evs.get(), MAX_EVENTS, -1);

		if (nevs < 0)
		{
			if (errno == EINTR)
			{
				continue;
			}
			LOG_ERROR << "wait epoll failed: " << ::strerror(errno);
			break;
		}
		else if (nevs == 0)
		{
			// LOG_WARN << "wait epoll outtime";
			continue;
		}
		// 获取过期回调事件

		for (size_t i = 0; i < nevs; i++)
		{
			epoll_event& ev = evs[i];

			if (ev.data.fd > 0)
			{
				auto it = std::find(wakeup_fds_.begin(), wakeup_fds_.end(),
									ev.data.fd);
				if (it != wakeup_fds_.end())
				{
					size_t distinct_idx =
						std::distance(wakeup_fds_.begin(), it);
					waitForEvent(distinct_idx);
					continue;
				}
			}

			FdContext* fd_ctx = reinterpret_cast<FdContext*>(ev.data.ptr);
			if (!fd_ctx)
			{
				continue;
			}

			if (ev.events & (EPOLLERR | EPOLLHUP))
			{
				ev.events |=
					(EPOLLIN | EPOLLOUT) & static_cast<uint8_t>(fd_ctx->events);
			}

			uint8_t real_events = 0;
			if (ev.events & EPOLLIN)
			{
				real_events |= static_cast<uint8_t>(IOManager::Event::kRead);
			}
			if (ev.events & EPOLLOUT)
			{
				real_events |= static_cast<uint8_t>(IOManager::Event::kWrite);
			}

			// if ((fd_ctx->events & real_events) == IOManager::Event::kNone)
			// {
			// 	continue;
			// }

			if (real_events == 0)
			{
				continue;
			}

			uint8_t left_events =
				static_cast<uint8_t>(fd_ctx->events) & (~real_events);

			if (left_events)
			{
				epoll_event nev;
				nev.events = EPOLLET | left_events;
				nev.data.ptr = fd_ctx;
				if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd_ctx->fd, &nev))
				{
					LOG_ERROR << "epoll_ctl MOD failed for fd=" << fd_ctx->fd
							  << ": " << ::strerror(errno);
				}
			}
			else
			{
				if (::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd_ctx->fd, nullptr))
				{
					LOG_ERROR << "epoll_ctl DEL failed for fd=" << fd_ctx->fd
							  << ": " << ::strerror(errno);
				}
			}

			size_t trigger_count = 0;
			if (real_events & static_cast<uint8_t>(IOManager::Event::kRead))
			{
				fd_ctx->triggerEvent(Event::kRead);
				trigger_count++;
			}
			if (real_events & static_cast<uint8_t>(IOManager::Event::kWrite))
			{
				fd_ctx->triggerEvent(Event::kWrite);
				trigger_count++;
			}

			if (trigger_count)
			{
				pending_event_count.fetch_sub(trigger_count,
											  std::memory_order_acq_rel);
			}
		}

		Fiber::curFiberPtr()->yield();
	}
}

bool IOManager::stopping()
{
	return pending_event_count.load(std::memory_order_acquire) == 0 &&
		   Scheduler::stopping();
}

// void IOManager::resetTimer()
// {
// 	tickle();
// }

bool IOManager::cancelEventImpl(FdContext* fd_ctx, Event new_events)
{
	if (!fd_ctx || fd_ctx->fd < 0)
	{
		LOG_ERROR << "invalid fd context";
		return false;
	}

	int op = !!(new_events) ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
	epoll_event ev;
	ev.events = EPOLLET | static_cast<uint8_t>(new_events);
	ev.data.ptr = fd_ctx;

	if (::epoll_ctl(epfd_, op, fd_ctx->fd, &ev))
	{
		LOG_ERROR << "epoll_ctl failed: fd=" << fd_ctx->fd << ", op=" << op
				  << ", err=" << ::strerror(errno);
		return false;
	}

	return true;
}

void IOManager::wakeupThread(int idx)
{
	int wakeup_fd = wakeup_fds_[idx];

	if (wakeup_fd < 0)
	{
		LOG_ERROR << "invalid wakeup fd: " << wakeup_fd;
		return;
	}

	uint64_t val = 1;
	ssize_t n = ::write(wakeup_fd, &val, sizeof(val));

	if (n != sizeof(val))
	{
		LOG_ERROR << "wakeup write failed: wrote " << n
				  << " bytes, err=" << ::strerror(errno);
	}
}

void IOManager::waitForEvent(int idx) // for wakeup
{
	int wakeup_fd = wakeup_fds_[idx];
	uint64_t val = 0;
	ssize_t n = ::read(wakeup_fd, &val, sizeof(val));

	if (n != sizeof(val))
	{
		LOG_ERROR << "handleRead failed: read " << n
				  << " bytes, err=" << ::strerror(errno);
	}
}