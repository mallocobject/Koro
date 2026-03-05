#include "koro/io_manager.h"
#include "elog/logger.h"
#include "koro/current_thread.h"
#include "koro/fiber.h"
#include "koro/scheduler.h"
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
		LOG_FATAL << "Unsupported event type";
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
	assert(events & event);
	events = events & (~event);

	EventContext& event_ctx = eventContext(event);
	if (event_ctx.cb)
	{
		event_ctx.scheduler->submit(event_ctx.cb);
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

	wakeup_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (wakeup_fd_ == -1)
	{
		LOG_FATAL << "create eventfd failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}

	epoll_event ev;
	ev.events = EPOLLIN || EPOLLOUT;
	ev.data.fd = wakeup_fd_;

	if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, wakeup_fd_, &ev))
	{
		LOG_FATAL << "add wakeup fd failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}

	init();
}

IOManager::~IOManager()
{
	stop();
	::close(epfd_);
	::close(wakeup_fd_);

	for (auto& item : fd_ctxes_)
	{
		if (item.second)
		{
			delete item.second;
		}
	}
}

IOManager* IOManager::localStance()
{
	return dynamic_cast<IOManager*>(Scheduler::localStance());
}

bool IOManager::addEvent(int fd, Event event, function cb)
{
	std::lock_guard<std::mutex> lock(mtx_);
	FdContext* fd_ctx = fd_ctxes_[fd];
	if (!!(fd_ctx->events & event))
	{
		return false;
	}

	int op = !!(fd_ctx->events) ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
	epoll_event ev;
	ev.events = static_cast<uint32_t>(EPOLLET | fd_ctx->events | event);
	ev.data.ptr = fd_ctx;

	if (::epoll_ctl(epfd_, op, fd, &ev))
	{
		LOG_ERROR << "add or modify event failed: " << ::strerror(errno);
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
		assert(event_ctx.fiber->state() == Fiber::State::kRunning);
	}

	return true;
}

bool IOManager::cancelEvent(int fd, Event event)
{
	std::lock_guard<std::mutex> lock(mtx_);

	FdContext* fd_ctx = fd_ctxes_.at(fd);
	if (!(fd_ctx->events & event))
	{
		return false;
	}

	Event new_events = fd_ctx->events & ~event;

	if (!cancelEventImpl(fd_ctx, new_events))
	{
		return false;
	}

	fd_ctx->events = new_events;
	FdContext::EventContext& event_ctx = fd_ctx->eventContext(event);
	FdContext::resetEventContext(event_ctx);
	pending_event_count.fetch_sub(1, std::memory_order_relaxed);

	if (!new_events)
	{
		fd_ctxes_.erase(fd);
	}
	return true;
}

bool IOManager::cancelEventAfterDone(int fd, Event event)
{
	std::lock_guard<std::mutex> lock(mtx_);

	FdContext* fd_ctx = fd_ctxes_.at(fd);
	if (!(fd_ctx->events & event))
	{
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
		fd_ctxes_.erase(fd);
	}
	return true;
}

bool IOManager::cancelEventAfterDone(int fd)
{
	std::lock_guard<std::mutex> lock(mtx_);

	FdContext* fd_ctx = fd_ctxes_.at(fd);
	if (!(fd_ctx->events))
	{
		return false;
	}
	if (!cancelEventImpl(fd_ctx, Event::kNone))
	{
		return false;
	}

	if (!!(fd_ctx->events & Event::kRead))
	{
		fd_ctx->triggerEvent(Event::kRead);
		pending_event_count.fetch_sub(1, std::memory_order_relaxed);
	}
	if (!!(fd_ctx->events & Event::kWrite))
	{
		fd_ctx->triggerEvent(Event::kWrite);
		pending_event_count.fetch_sub(1, std::memory_order_relaxed);
	}

	assert(fd_ctx->events);
	fd_ctxes_.erase(fd);
	return true;
}

void IOManager::tickle()
{
	if (hasIdleThread())
	{
		wakeup();
	}
}

void IOManager::idle()
{
	static const size_t MAX_EVENTS = 256;
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

		int nevs = ::epoll_wait(epfd_, evs.get(), MAX_EVENTS, -1);

		if (nevs < 0 && errno != EINTR)
		{
			LOG_ERROR << "wait epoll failed: " << ::strerror(errno);
		}
		else if (nevs == 0)
		{
			LOG_WARN << "wait epoll outtime";
		}
		// 获取过期回调事件

		for (size_t i = 0; i < nevs; i++)
		{
			epoll_event& ev = evs[i];

			if (ev.data.fd == wakeup_fd_)
			{
				handleRead();
				continue;
			}

			FdContext* fd_ctx = reinterpret_cast<FdContext*>(ev.data.ptr);

			if (ev.events & (EPOLLERR | EPOLLHUP))
			{
				ev.events |=
					(EPOLLIN | EPOLLOUT) & static_cast<uint8_t>(fd_ctx->events);
			}

			uint8_t real_events = static_cast<uint8_t>(IOManager::Event::kNone);
			if (ev.events & EPOLLIN)
			{
				real_events |= static_cast<uint8_t>(IOManager::Event::kRead);
			}
			if (ev.events & EPOLLOUT)
			{
				real_events |= static_cast<uint8_t>(IOManager::Event::kWrite);
			}

			if ((fd_ctx->events & real_events) == IOManager::Event::kNone)
			{
				continue;
			}

			uint8_t left_events =
				static_cast<uint8_t>(fd_ctx->events) & (~real_events);
			int op = left_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
			ev.events = EPOLLET | left_events;

			if (::epoll_ctl(epfd_, op, fd_ctx->fd, &ev))
			{
				LOG_ERROR << "modify or delete event failed: "
						  << ::strerror(errno);
				continue;
			}

			if (real_events & static_cast<uint8_t>(IOManager::Event::kRead))
			{
				fd_ctx->triggerEvent(Event::kRead);
				pending_event_count.fetch_sub(1, std::memory_order_acq_rel);
			}
			if (real_events & static_cast<uint8_t>(IOManager::Event::kWrite))
			{
				fd_ctx->triggerEvent(Event::kWrite);
				pending_event_count.fetch_sub(1, std::memory_order_acq_rel);
			}
		}
		Fiber::curFiberPtr()->yield();
	}
}

bool IOManager::stopping()
{
	return true;
}

void IOManager::resetTimer()
{
	tickle();
}

bool IOManager::cancelEventImpl(FdContext* fd_ctx, Event new_events)
{

	int op = !!(new_events) ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
	epoll_event ev;
	ev.events = static_cast<uint32_t>(EPOLLET | new_events);
	ev.data.ptr = fd_ctx;

	if (::epoll_ctl(epfd_, op, fd_ctx->fd, &ev))
	{
		LOG_ERROR << "delete event failed: " << ::strerror(errno);
		return false;
	}

	return true;
}

void IOManager::wakeup()
{
	uint64_t signal = 1;
	ssize_t n = ::write(wakeup_fd_, &signal, sizeof(signal));

	if (n != sizeof(signal))
	{
		LOG_ERROR << "EventLoop::wakeup() writes " << n
				  << " bytes instead of 8";
	}
}

void IOManager::handleRead() // for wakeup
{
	uint64_t signal = 1;
	ssize_t n = ::read(wakeup_fd_, &signal, sizeof(signal));

	if (n != sizeof(signal))
	{
		LOG_ERROR << "EventLoop::handleRead() reads " << n
				  << " bytes instead of 8";
	}
}