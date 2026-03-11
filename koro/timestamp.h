#ifndef KORO_TIMESTAMP_H
#define KORO_TIMESTAMP_H

#include <chrono>
#include <compare>
#include <fcntl.h>
#include <sys/time.h>
namespace koro
{
struct Timestamp
{
	using Clock = std::chrono::steady_clock;
	using TimePoint = Clock::time_point;

	TimePoint tp;

	itimerspec toItimerspec() const
	{
		auto ns_since_epoch =
			std::chrono::duration_cast<std::chrono::nanoseconds>(
				tp.time_since_epoch())
				.count();
		itimerspec its;
		its.it_value.tv_sec = ns_since_epoch / 1'000'000'000;
		its.it_value.tv_nsec = ns_since_epoch % 1'000'000'000;
		return its;
	}

	itimerspec toRelativeItimerspec() const
	{
		auto now = Clock::now();
		auto diff = tp - now;

		itimerspec its{};
		if (diff <= std::chrono::duration<int64_t>::zero())
		{
			its.it_value.tv_sec = 0;
			its.it_value.tv_nsec = 0;
		}
		else
		{
			auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(diff)
						  .count();
			its.it_value.tv_sec = ns / 1'000'000'000;
			its.it_value.tv_nsec = ns % 1'000'000'000;
		}
		its.it_interval.tv_sec = 0;
		its.it_interval.tv_nsec = 0;
		return its;
	}

	explicit Timestamp(TimePoint t = Clock::now()) : tp(t)
	{
	}

	std::strong_ordering operator<=>(const Timestamp& other) const
	{
		return tp <=> other.tp;
	}

	bool operator==(const Timestamp& other) const
	{
		return (*this <=> other) == 0;
	}

	Timestamp operator+(double sec) const
	{
		return Timestamp(
			TimePoint(std::chrono::duration_cast<std::chrono::nanoseconds>(
				std::chrono::duration<double>(sec) + tp.time_since_epoch())));
	}

	static Timestamp now()
	{
		return Timestamp();
	}
};
} // namespace koro

#endif