#ifndef KORO_TASK_H
#define KORO_TASK_H

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
namespace koro
{
class Fiber;
class EpollPoller;
class Channel;
class TimerManager;
struct ScheduledTask
{
	using function = std::function<void()>;

	std::shared_ptr<Fiber> fiber;
	function cb;

	ScheduledTask()
	{
	}

	ScheduledTask(ScheduledTask&& other)
		: fiber(other.fiber), cb(std::move(other.cb))
	{
		other.fiber = nullptr;
		other.cb = nullptr;
	}

	ScheduledTask& operator=(ScheduledTask&& other)
	{
		if (this == &other)
		{
			return *this;
		}

		fiber = other.fiber;
		cb = std::move(other.cb);

		other.fiber = nullptr;
		other.cb = nullptr;

		return *this;
	}

	ScheduledTask(std::shared_ptr<Fiber> f) : fiber(f)
	{
	}

	ScheduledTask(std::shared_ptr<Fiber>* f)
	{
		fiber.swap(*f);
	}

	ScheduledTask(function f) : cb(std::move(f))
	{
	}

	ScheduledTask(function* f)
	{
		cb.swap(*f);
	}

	void clear()
	{
		fiber = nullptr;
		cb = nullptr;
	}

	explicit operator bool() const noexcept
	{
		return fiber || cb;
	}
};

struct TaskQueue
{
	std::deque<std::shared_ptr<ScheduledTask>> tasks;
	std::mutex mtx;
	std::condition_variable cv;
	std::atomic<bool> idling{false};
	std::shared_ptr<EpollPoller> epoller_;
	std::shared_ptr<Channel> wakeup_ch_;
	std::shared_ptr<TimerManager> tm_;
	Channel* timer_ch_;
};
} // namespace koro

#endif