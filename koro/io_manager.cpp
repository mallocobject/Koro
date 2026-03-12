#include "koro/io_manager.h"
#include "elog/logger.h"
#include "koro/channel.h"
#include "koro/epoll_poller.h"
#include "koro/fiber.h"
#include "koro/file_descriptor.h"
#include "koro/scheduler.h"
#include "koro/task.h"
#include "koro/timer_manager.h"
#include "koro/timestamp.h"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sys/epoll.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace koro;

IOManager::IOManager(size_t thread_count) : Scheduler(thread_count)
{
}

IOManager::~IOManager()
{
	stop();

	assert(stop_.load(std::memory_order_acquire));
}

std::shared_ptr<Channel> IOManager::bindTaskQueue(int fd)
{
	init();

	std::unique_lock<std::shared_mutex> lock(ctable.mtx);
	while (fd >= ctable.chs.size())
	{
		ctable.chs.resize(std::max<size_t>(fd * 1.5, fd + 16));
	}

	std::shared_ptr<Channel> ch = ctable.chs[fd];
	if (!ch)
	{
		if (!t_task_queue)
		{
			size_t idx =
				thread_to_post_index_.fetch_add(1, std::memory_order_relaxed) %
				threads_.size();
			ch = std::make_shared<Channel>(fd, task_queues_[idx].get());
		}
		else
		{
			ch = std::make_shared<Channel>(fd, t_task_queue);
		}

		ctable.chs[fd] = ch;
	}

	return ch;
}

bool IOManager::registerEvent(std::shared_ptr<Channel> ch, Event e,
							  const std::function<void()>& cb, bool useET,
							  int timeout)
{
	assert(ch->taskQueue());
	uint32_t event = static_cast<uint32_t>(e);
	{
		std::lock_guard<std::mutex> lock(ch->mtx_);
		if (event == 0x001)
		{
			ch->setReadCallback(cb);
		}
		else if (event == 0x004)
		{
			ch->setWriteCallback(cb);
		}
		else
		{
			LOG_WARN << "fd: " << ch->fd()
					 << " registers unsupportted event: " << event;
			return false;
		}

		std::weak_ptr<Channel> wch = ch;
		ch->setErrorCallback(
			[this, wch]
			{
				if (auto share_ch = wch.lock())
				{
					handleError(share_ch);
				}
			});

		ch->setEvents(ch->events() | event);
		if (useET)
		{
			ch->useET();
		}
		ch->update();
	}

	pending_event_count_.fetch_add(1, std::memory_order_release);

	return true;
}

void IOManager::removeChannel(std::shared_ptr<Channel> ch)
{
	if (!ch->taskQueue())
	{
		return;
	}

	int fd = ch->fd();

	{
		std::unique_lock<std::shared_mutex> lock(ctable.mtx);
		if (fd < ctable.chs.size() && ctable.chs[fd])
		{
			ctable.chs[fd].reset();
		}
	}

	// ch->disableAll();
	ch->setReadCallback(nullptr);
	ch->setWriteCallback(nullptr);
	ch->setErrorCallback(nullptr);
	if (ch->inEpoll())
	{
		ch->setEvents(0);
		ch->remove();
	}
}

void IOManager::unregisterEvent(std::shared_ptr<Channel> ch, Event e)
{
	assert(ch->taskQueue());
	uint32_t event = static_cast<uint32_t>(e);

	size_t ban_event_count = 0;

	{
		std::lock_guard<std::mutex> lock(ch->mtx_);
		if (event & 0x001)
		{
			// ch->disableReading();
			ch->setEvents(ch->events() & ~0x001);
			ch->setReadCallback(nullptr);
			ban_event_count++;
		}
		if (event & 0x004)
		{
			// ch->disableWriting();
			ch->setEvents(ch->events() & ~0x004);
			ch->setWriteCallback(nullptr);
			ban_event_count++;
		}

		ch->update();
	}

	if (ban_event_count > 0)
	{
		pending_event_count_.fetch_sub(ban_event_count,
									   std::memory_order_release);
	}
}

void IOManager::onInit()
{
	for (auto& task_queue : task_queues_)
	{
		task_queue->epoller_ = std::make_unique<EpollPoller>();
		int wakeup_fd = FD::createEventFd();
		auto wakeup_ch = std::make_unique<Channel>(wakeup_fd, task_queue.get());
		wakeup_ch->enableReading();
		task_queue->wakeup_ch_ = std::move(wakeup_ch);

		auto tm = std::make_unique<TimerManager>(task_queue.get());
		task_queue->timer_ch_ = tm->ch();
		task_queue->tm_ = std::move(tm);
	}
}

void IOManager::idle(size_t idx)
{
	TaskQueue& task_queue = *task_queues_[idx];
	std::vector<epoll_event> active_evs;
	while (true)
	{
		active_evs.clear();
		task_queue.idling.store(true, std::memory_order_seq_cst);
		if (stopping())
		{
			task_queue.idling.store(false, std::memory_order_release);
			LOG_WARN << "thread: " << idx << " quit";
			break;
		}

		bool has_task = false;
		{
			std::lock_guard<std::mutex> lock(task_queue.mtx);
			has_task = !task_queue.tasks.empty();
		}
		if (has_task)
		{
			task_queue.idling.store(false, std::memory_order_release);
			Fiber::runningFiber()->yield();
			continue;
		}

		static const int kMaxTimeoutMs = -1;
		LOG_TRACE << "epoll: " << idx << " enter waiting state";
		task_queue.epoller_->poll(&active_evs, kMaxTimeoutMs);
		LOG_TRACE << "epoll: " << idx << " out of waiting state";
		task_queue.idling.store(false, std::memory_order_release);

		{
			for (auto& ev : active_evs)
			{
				int fd = ev.data.fd;
				uint32_t revents = ev.events;
				if (task_queue.wakeup_ch_ && task_queue.wakeup_ch_->fd() == fd)
				{
					FD::readEventFd(fd);
				}
				else if (task_queue.timer_ch_ &&
						 task_queue.timer_ch_->fd() == fd)
				{
					task_queue.timer_ch_->onTime();
				}
				else
				{
					std::shared_ptr<Channel> ch;
					{
						std::shared_lock<std::shared_mutex> lock(ctable.mtx);
						if (fd >= 0 && fd < ctable.chs.size())
						{
							ch = ctable.chs[fd];
						}
					}
					if (ch)
					{
						ch->setReadyEvent(revents);
						ch->handleEvent();
					}
				}
			}
		}

		if (stopping())
		{
			LOG_WARN << "thread: " << idx << " quit";
			break;
		}

		Fiber::runningFiber()->yield();
	}
}

void IOManager::tickle(size_t idx)
{
	TaskQueue& task_queue = *task_queues_[idx];
	if (task_queue.idling.load(std::memory_order_seq_cst))
	{
		FD::writeEventFd(task_queue.wakeup_ch_->fd());
	}
}

void IOManager::handleError(std::shared_ptr<Channel> ch)
{
}

TimerId IOManager::runAt(Timestamp timestamp, const std::function<void()>& cb)
{
	assert(t_task_queue);
	pending_event_count_.fetch_add(1, std::memory_order_release);
	return t_task_queue->tm_->registerTimer(timestamp, cb, -1);
}

TimerId IOManager::runAfter(double delay_sec, const std::function<void()>& cb)
{
	assert(t_task_queue);
	pending_event_count_.fetch_add(1, std::memory_order_release);
	return t_task_queue->tm_->registerTimer(Timestamp::now() + delay_sec, cb,
											-1);
}

TimerId IOManager::runEvery(double interval_sec,
							const std::function<void()>& cb)
{
	assert(t_task_queue);
	pending_event_count_.fetch_add(1, std::memory_order_release);
	return t_task_queue->tm_->registerTimer(Timestamp::now() + interval_sec, cb,
											interval_sec);
}

void IOManager::cancellTimer(TimerId timer_id)
{
	assert(t_task_queue);
	pending_event_count_.fetch_sub(1, std::memory_order_release);
	t_task_queue->tm_->unregisterEvent(timer_id);
}