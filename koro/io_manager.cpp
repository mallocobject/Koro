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

std::shared_ptr<Channel> IOManager::bindTaskQueue(int fd)
{
	size_t idx = thread_to_post_index_.fetch_add(1, std::memory_order_relaxed) %
				 threads_.size();
	TaskQueue& task_queue = *task_queues_[idx];
	return std::make_shared<Channel>(fd, &task_queue);
}

bool IOManager::registerEvent(std::shared_ptr<Channel> ch, Event e,
							  std::shared_ptr<ScheduledTask> cb, bool useET)
{
	assert(ch->taskQueue());
	uint32_t event = static_cast<uint32_t>(e);
	{
		std::lock_guard<std::mutex> lock(ch->taskQueue()->mtx);
		if (event == 0x001)
		{
			ch->setReadCallback(std::move(cb));
		}
		else if (event == 0x004)
		{
			ch->setWriteCallbakc(std::move(cb));
		}
		else
		{
			LOG_WARN << "fd: " << ch->fd()
					 << " registers unsupportted event: " << event;
			return false;
		}

		ch->setErrorCallback(std::make_shared<ScheduledTask>(
			std::bind(&IOManager::handleError, this, ch->fd())));

		ch->setEvents(event);
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
	TaskQueue& task_queue = *ch->taskQueue();

	{
		std::lock_guard<std::mutex> lock(task_queue.mtx);
		if (auto it = task_queue.chs_.find(fd); it != task_queue.chs_.end())
		{
			assert(it->first == fd && it->second == ch);
			task_queue.chs_.erase(it);
		}
	}

	// ch->disableAll();
	ch->setEvents(0);
	ch->remove();
}

void IOManager::unregisterEvent(std::shared_ptr<Channel> ch, Event e)
{
	assert(ch->taskQueue());
	uint32_t event = static_cast<uint32_t>(e);

	TaskQueue& task_queue = *ch->taskQueue();
	size_t ban_event_count = 0;

	{
		std::lock_guard<std::mutex> lock(task_queue.mtx);
		if (event & 0x001)
		{
			// ch->disableReading();
			ch->setEvents(event & ~0x001);
			ban_event_count++;
		}
		if (event & 0x004)
		{
			// ch->disableWriting();
			ch->setEvents(event & ~0x004);
			ban_event_count++;
		}

		ch->update();
	}

	pending_event_count_.fetch_sub(ban_event_count, std::memory_order_release);
}

// void IOManager::unregisterEventAfterDone(Channel* ch, Event e)
// {
// 	int fd = ch->fd();
// 	TaskQueue& task_queue = *ch->taskQueue();
// 	std::lock_guard<std::mutex> lock(task_queue.mtx);
// 	auto it = task_queue.chs_.find(fd);
// 	if (it != task_queue.chs_.end())
// 	{
// 		assert(it->second.get() == ch);

// 		uint32_t event = static_cast<uint32_t>(e);
// 		if (event & 0x001)
// 		{
// 			ch->disableReading();
// 			ch->triggerEvent(0x001);
// 			pending_event_count_.fetch_sub(1, std::memory_order_release);
// 		}
// 		if (event & 0x004)
// 		{
// 			ch->disableWriting();
// 			ch->triggerEvent(0x004);
// 			pending_event_count_.fetch_sub(1, std::memory_order_release);
// 		}
// 	}
// }

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
		active_chs.clear();
		task_queue.idling.store(true, std::memory_order_seq_cst);
		if (stopping())
		{
			task_queue.idling.store(false, std::memory_order_release);
			LOG_WARN << "thread: " << idx << " quit";
			break;
		}

		static const int kMaxTimeoutMs = -1;
		LOG_DEBUG << "epoll: " << idx << " enter waiting state";
		task_queue.epoller_->poll(&active_chs, kMaxTimeoutMs);
		LOG_DEBUG << "epoll: " << idx << " out of waiting state";
		task_queue.idling.store(false, std::memory_order_release);

		{
			for (auto& ch : active_chs)
			{

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
					std::lock_guard<std::mutex> lock(task_queue.mtx);
					ch->handleEvent();
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
	uint64_t one = 1;
	TaskQueue& task_queue = *task_queues_[idx];
	if (task_queue.idling.load(std::memory_order_seq_cst))
	{
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