#ifndef KORO_SCHEDULER_H
#define KORO_SCHEDULER_H

#include "koro/inplace_function.hpp"
#include "koro/noncopyable.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>
namespace koro
{
class Fiber;
class Scheduler : public noncopyable
{
	using function = InplaceFunction<64>;

  private:
	struct ScheduledTask
	{
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

	struct Data
	{
		std::deque<ScheduledTask> tasks;
		std::mutex mtx;
		std::condition_variable cv;
	};

  private:
	std::mutex mtx_;

	std::vector<std::thread> threads_;
	std::vector<std::unique_ptr<Data>> data_;
	std::atomic<size_t> thread_to_post_index_{0};

	std::atomic<size_t> active_thread_count_{0};

	std::atomic<bool> stop_{false};

  public:
	Scheduler(size_t thread_count = 1);
	virtual ~Scheduler();

	template <typename CF>
		requires std::is_same_v<std::decay_t<CF>, std::shared_ptr<Fiber>> ||
				 std::invocable<CF> &&
					 std::same_as<std::invoke_result_t<CF>, void>
				 void submit(CF&& cf)
	{
		bool need_tickle = false;

		// fetch_add return old val
		// xxx algorithm
		auto& data = *data_[thread_to_post_index_.fetch_add(
								1, std::memory_order_acq_rel) %
							threads_.size()];
		{
			std::lock_guard<std::mutex> lock(data.mtx);
			need_tickle = data.tasks.empty();
			ScheduledTask task(cf);
			if (task.fiber || task.cb)
			{
				data.tasks.push_back(std::move(task));
			}
		}

		if (need_tickle)
		{
			data.cv.notify_one();
		}
	}

	ScheduledTask stealTask(size_t thief_id);

	virtual void init();
	virtual void stop();

  protected:
	void setLocalStance();

	virtual void tickle();
	virtual void run(size_t thread_index);
	virtual void idle();

	virtual bool stopping()
	{
		std::lock_guard<std::mutex> lock(mtx_);
		for (const auto& data : data_)
		{
			stop_ = stop_ && data->tasks.empty();
		}
		return stop_ && (active_thread_count_ == 0);
	}

	bool hasIdleThread() const
	{
		return threads_.size() >
			   active_thread_count_.load(std::memory_order_relaxed);
	}

  public:
	static Scheduler* localStance();
};
} // namespace koro

#endif