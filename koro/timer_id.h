#ifndef KORO_TIMER_ID_H
#define KORO_TIMER_ID_H

#include <atomic>
#include <cstdint>
#include <memory>
namespace koro
{
class Timer;
class TimerId
{
  private:
	std::weak_ptr<Timer> timer_;
	uint64_t id_{0};

	inline static std::atomic<uint64_t> id_creator_{1};

  public:
	TimerId()
	{
	}

	TimerId(std::weak_ptr<Timer> timer) : timer_(timer)
	{
		id_ = id_creator_.fetch_add(1, std::memory_order_acq_rel);
	}

	TimerId(const TimerId&) = default;

	bool isAlive() const
	{
		return !timer_.expired();
	}

	std::shared_ptr<Timer> timer() const
	{
		return timer_.lock();
	}

	uint64_t id() const
	{
		return id_;
	}
};

} // namespace koro

#endif