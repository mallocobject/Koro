#include "koro/timer_manager.h"
#include "elog/logger.h"
#include "koro/channel.h"
#include "koro/file_descriptor.h"
#include "koro/task.h"
#include "koro/timer.h"
#include "koro/timer_id.h"
#include "koro/timestamp.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <strings.h>
#include <sys/time.h>
#include <sys/timerfd.h>

using namespace koro;

TimerManager::TimerManager(TaskQueue* task_queue) : task_queue_(task_queue)
{
	int fd = FD::createTimerFd();
	ch_ = std::make_unique<Channel>(fd, task_queue);
	ch_->setOnTimeCallback(std::bind(&TimerManager::triggerTimer, this));
	ch_->enableReading();
}

TimerManager::~TimerManager()
{
	ch_->disableAll();
	ch_->remove();

	timers_.clear();
}

TimerId TimerManager::registerTimer(Timestamp timestamp,
									const std::shared_ptr<ScheduledTask>& cb,
									double interval)
{
	std::shared_ptr<Timer> timer(new Timer(timestamp, cb, interval));
	if (insert(timer))
	{
		resetTimerFd(timer);
	}

	timer->enable(true);
	return TimerId(timer);
}

void TimerManager::unregisterEvent(TimerId timer_id)
{
	auto timer = timer_id.timer();
	if (timer_id.id() == 0)
	{
		LOG_WARN << "timer id invalid: cannot be zero(0)";
		return;
	}
	else if (!timer)
	{
		LOG_WARN << "timer is null";
		return;
	}

	timer->enable(false);
}

void TimerManager::triggerTimer()
{
	FD::readEventFd(ch_->fd());

	auto max_share_ptr = std::shared_ptr<Timer>(
		reinterpret_cast<Timer*>(UINTPTR_MAX), [](Timer*) {});

	auto end = timers_.upper_bound(Entry{Timestamp::now(), max_share_ptr});
	active_timers_.insert(active_timers_.end(), timers_.begin(), end);
	timers_.erase(timers_.begin(), end);

	for (auto& e : active_timers_)
	{
		auto timer = e.second;
		if (timer->on())
		{
			std::lock_guard<std::mutex> q_lock(task_queue_->mtx);
			task_queue_->tasks.push_back(timer->onTimeCallback());
		}
	}
	resetTimer();
}

bool TimerManager::insert(std::shared_ptr<Timer> timer)
{
	bool reset_instantly = false;
	if (timers_.empty() || timer->expiration() < timers_.begin()->first)
	{
		reset_instantly = true;
	}

	Entry e{timer->expiration(), timer};
	timers_.insert(e);
	return reset_instantly;
}

void TimerManager::resetTimer()
{
	for (auto e : active_timers_)
	{
		auto timer = e.second;
		if (timer->on() && timer->repeating())
		{
			timer->repeat();
			insert(timer);
		}
	}

	active_timers_.clear();

	if (!timers_.empty())
	{
		resetTimerFd(timers_.begin()->second);
	}
}

void TimerManager::resetTimerFd(std::shared_ptr<Timer> timer)
{
	itimerspec value = timer->expiration().toRelativeItimerspec();
	int ret = ::timerfd_settime(ch_->fd(), 0, &value, nullptr);
	if (ret == -1)
	{
		LOG_ERROR << "time setting faild - fd: " << ch_->fd() << " : "
				  << strerror(errno);
	}
}