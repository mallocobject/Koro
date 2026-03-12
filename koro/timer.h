#ifndef KORO_TIMER_H
#define KORO_TIMER_H

#include "koro/noncopyable.h"
#include "koro/timestamp.h"
#include <atomic>
#include <functional>
namespace koro
{
class TimerManager;
class Timer : public noncopyable
{
	friend class TimerManager;

  private:
	Timestamp expiration_;
	double interval_{-1};
	bool repeat_{false};
	std::atomic<bool> on_{false};
	std::function<void()> on_time_callback_;

  private:
	explicit Timer(Timestamp expiration, std::function<void()> cb,
				   double interval)
		: expiration_(expiration), on_time_callback_(std::move(cb)),
		  interval_(interval), repeat_(interval > 0.0)
	{
	}

  public:
	~Timer()
	{
	}

	bool repeating() const
	{
		return repeat_;
	}

	bool on() const
	{
		return on_.load(std::memory_order_acquire);
	}

	void enable(bool on = true)
	{
		on_.store(on, std::memory_order_release);
	}

	Timestamp expiration() const
	{
		return expiration_;
	}

	// std::function<void()> onTimeCallback()
	// {
	// 	return on_time_callback_;
	// }

	void repeat()
	{
		expiration_ = expiration_ + interval_;
	}
};
} // namespace koro

#endif