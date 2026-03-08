#include "koro/io_manager.h"
#include "elog/logger.h"
#include "koro/channel.h"
#include "koro/epoll_poller.h"
#include "koro/fiber.h"
#include "koro/file_descriptor.h"
#include "koro/scheduler.h"
#include "koro/task.h"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace koro;

IOManager::IOManager(size_t thread_count) : Scheduler(thread_count)
{
}

IOManager::~IOManager()
{
}

Channel* IOManager::registerEvent(int fd, Event e,
								  std::shared_ptr<ScheduledTask> cb, bool useET)
{
	size_t idx = thread_to_post_index_.fetch_add(1, std::memory_order_relaxed) %
				 threads_.size();
	TaskQueue& task_queue = *task_queues_[idx];
	std::lock_guard<std::mutex> lock(task_queue.mtx);

	if (task_queue.chs_.find(fd) != task_queue.chs_.end())
	{
		LOG_ERROR << "fd " << fd << " already exists in IOManager";
		return nullptr;
	}

	auto ch = std::make_unique<Channel>(fd, &task_queue);

	uint32_t event = static_cast<uint32_t>(e);
	if (event == 0x001)
	{
		ch->setReadCallback(std::move(cb));
	}
	else if (event == 0x004)
	{
		ch->setWriteCallbakc(std::move(cb));
	}

	ch->setErrorCallback(std::make_shared<ScheduledTask>(
		std::bind(&IOManager::handleError, this, ch->fd())));

	ch->setEvents(event);
	if (useET)
	{
		ch->useET();
	}
	ch->update();

	Channel* raw = ch.get();
	task_queue.chs_[fd] = std::move(ch);

	pending_event_count_.fetch_add(1, std::memory_order_release);

	return raw;
}

void IOManager::removeChannel(Channel* ch)
{
	int fd = ch->fd();
	TaskQueue& task_queue = *ch->taskQueue();
	std::lock_guard<std::mutex> lock(task_queue.mtx);
	auto it = task_queue.chs_.find(fd);
	if (it != task_queue.chs_.end())
	{
		assert(it->second.get() == ch);
		ch->disableAll();
		ch->remove();
		task_queue.chs_.erase(it);
	}
}

void IOManager::unregisterEvent(Channel* ch, Event e)
{
	int fd = ch->fd();
	TaskQueue& task_queue = *ch->taskQueue();
	std::lock_guard<std::mutex> lock(task_queue.mtx);
	auto it = task_queue.chs_.find(fd);
	if (it != task_queue.chs_.end())
	{
		assert(it->second.get() == ch);

		uint32_t event = static_cast<uint32_t>(e);
		if (event & 0x001)
		{
			ch->disableReading();
			pending_event_count_.fetch_sub(1, std::memory_order_release);
		}
		if (event & 0x004)
		{
			ch->disableWriting();
			pending_event_count_.fetch_sub(1, std::memory_order_release);
		}
	}
}

void IOManager::unregisterEventAfterDone(Channel* ch, Event e)
{
	int fd = ch->fd();
	TaskQueue& task_queue = *ch->taskQueue();
	std::lock_guard<std::mutex> lock(task_queue.mtx);
	auto it = task_queue.chs_.find(fd);
	if (it != task_queue.chs_.end())
	{
		assert(it->second.get() == ch);

		uint32_t event = static_cast<uint32_t>(e);
		if (event & 0x001)
		{
			ch->disableReading();
			ch->triggerEvent(0x001);
			pending_event_count_.fetch_sub(1, std::memory_order_release);
		}
		if (event & 0x004)
		{
			ch->disableWriting();
			ch->triggerEvent(0x004);
			pending_event_count_.fetch_sub(1, std::memory_order_release);
		}
	}
}

void IOManager::onInit()
{
	for (auto& task_queue : task_queues_)
	{
		task_queue->epoller_ = std::make_shared<EpollPoller>();
		int wakeup_fd = FD::createEventFd();
		auto wakeup_ch = std::make_shared<Channel>(wakeup_fd, task_queue.get());
		wakeup_ch->enableReading();
		task_queue->wakeup_ch_ = std::move(wakeup_ch);
	}
}

void IOManager::idle(size_t idx)
{
	TaskQueue& task_queue = *task_queues_[idx];
	std::vector<Channel*> active_chs;
	while (true)
	{

		if (stopping())
		{
			break;
		}

		active_chs.clear();

		task_queue.idling.store(true, std::memory_order_seq_cst);
		static const int kMaxTimeoutMs = 500;
		task_queue.epoller_->poll(&active_chs, kMaxTimeoutMs);

		for (auto& ch : active_chs)
		{
			std::lock_guard<std::mutex> lock(task_queue.mtx);
			if (ch == task_queue.wakeup_ch_.get())
			{
				uint64_t one = 1;
				ssize_t n = ::read(ch->fd(), &one, sizeof(one));

				if (n != sizeof(one))
				{
					LOG_ERROR << "wakeup_fd reads " << n
							  << " bytes instead of 8";
				}
			}
			else
			{
				ch->handleEvent();
			}
		}

		if (stopping())
		{
			break;
		}

		task_queue.idling.store(false, std::memory_order_release);
		Fiber::runningFiber()->yield();
	}
}

void IOManager::tickle(size_t idx)
{
	uint64_t one = 1;
	TaskQueue& task_queue = *task_queues_[idx];
	if (task_queue.idling.load(std::memory_order_seq_cst))
	{
		std::lock_guard<std::mutex> lock(task_queue.mtx);
		ssize_t n = ::write(task_queue.wakeup_ch_->fd(), &one,
							sizeof(one)); // 唤醒 epoll_wait

		if (n != sizeof(one))
		{
			LOG_ERROR << "wakeup_fd writes " << n << " bytes instead of 8";
		}
	}
}

void IOManager::handleError(int fd)
{
}