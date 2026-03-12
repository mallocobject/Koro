#include "koro/channel.h"
#include "elog/logger.h"
#include "koro/epoll_poller.h"
#include "koro/task.h"
#include <cassert>
#include <functional>
#include <sys/epoll.h>
#include <unistd.h>

using namespace koro;

Channel::Channel(int fd, TaskQueue* task_queue)
	: fd_(fd), task_queue_(task_queue)
{
}

Channel::~Channel()
{
}

void Channel::remove()
{
	assert(task_queue_ && task_queue_->epoller_);
	task_queue_->epoller_->removeChannel(this);
	task_queue_ = nullptr;
}

void Channel::update()
{
	assert(task_queue_ && task_queue_->epoller_);
	task_queue_->epoller_->updateChannel(this);
}

void Channel::handleEvent()
{
	if (revents_ & (EPOLLERR | EPOLLHUP))
	{
		revents_ |= EPOLLIN | EPOLLOUT;
	}
	if (revents_ & EPOLLERR)
	{
		triggerEvent(EPOLLERR);
	}
	if (revents_ & EPOLLIN)
	{
		triggerEvent(EPOLLIN);
	}
	if (revents_ & EPOLLOUT)
	{
		triggerEvent(EPOLLOUT);
	}
}

void Channel::triggerEvent(uint32_t e)
{
	std::function<void()> cb;
	{
		std::lock_guard<std::mutex> lock(mtx_);
		switch (e)
		{
		case EPOLLIN:
			cb = read_callback_;
			break;
		case EPOLLOUT:
			cb = write_callback_;
			break;
		case EPOLLERR:
			cb = error_callback_;
			break;
		default:
			LOG_ERROR << "unsupported event triggers: " << e;
			return;
		}
	}
	if (cb)
	{
		std::lock_guard<std::mutex> q_lock(task_queue_->mtx);
		task_queue_->tasks.push_back(std::make_shared<ScheduledTask>(cb));
	}
}