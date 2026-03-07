#ifndef KORO_TASK_H
#define KORO_TASK_H

#include <functional>
#include <memory>
namespace koro
{
class Fiber;
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
};
} // namespace koro

#endif