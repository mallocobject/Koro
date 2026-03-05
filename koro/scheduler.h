#ifndef KORO_SCHEDULER_H
#define KORO_SCHEDULER_H

#include "koro/noncopyable.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>
namespace koro
{
class Fiber;
class Scheduler : public noncopyable
{
  private:
	struct ScheduledTask
	{
		std::shared_ptr<Fiber> fiber;
		std::function<void()> cb;
		std::thread::id tid;

		ScheduledTask()
		{
		}

		ScheduledTask(ScheduledTask&& other)
			: fiber(other.fiber), cb(std::move(other.cb)), tid(other.tid)
		{
			other.fiber = nullptr;
			other.cb = nullptr;
			tid = std::thread::id();
		}

		ScheduledTask& operator=(ScheduledTask&& other)
		{
			if (this == &other)
			{
				return *this;
			}

			fiber = other.fiber;
			cb = other.cb;
			tid = other.tid;

			other.fiber = nullptr;
			other.cb = nullptr;
			tid = std::thread::id();

			return *this;
		}

		ScheduledTask(std::shared_ptr<Fiber> f, std::thread::id t)
			: fiber(f), tid(t)
		{
		}

		ScheduledTask(std::shared_ptr<Fiber>* f, std::thread::id t) : tid(t)
		{
			fiber.swap(*f);
		}

		ScheduledTask(std::function<void()> f, std::thread::id t)
			: cb(std::move(f)), tid(t)
		{
		}

		ScheduledTask(std::function<void()>* f, std::thread::id t) : tid(t)
		{
			cb.swap(*f);
		}

		void clear()
		{
			fiber = nullptr;
			cb = nullptr;
			tid = std::thread::id();
		}
	};

	// struct Data
	// {
	// 	std::deque<ScheduledTask> queue;
	// 	std::mutex mtx;
	// 	std::condition_variable cv;
	// };

  private:
	std::mutex mtx_;
	std::condition_variable cv_;

	std::vector<std::shared_ptr<std::thread>> threads_;
	std::queue<ScheduledTask> tasks_;
	// std::vector<std::thread> threads_;
	// std::vector<std::unique_ptr<Data>> data_;
	std::atomic<size_t> next_thread_index_{0};

	// size_t thread_count_{0};
	std::atomic<size_t> active_thread_count_{0};
	std::atomic<size_t> idle_thread_count_{0};

	std::atomic<bool> stop_{false};

  public:
	Scheduler(size_t thread_num = 1);
	virtual ~Scheduler();

	template <typename F>
		requires std::is_same_v<std::decay_t<F>, std::shared_ptr<Fiber>> ||
				 std::invocable<F> &&
					 std::same_as<std::invoke_result_t<F>, void>
				 void postTask(F f, std::thread::id tid = std::thread::id())
	{
		bool need_tickle = false;
		{
			std::lock_guard<std::mutex> lock(mtx_);
			need_tickle = tasks_.empty();
			ScheduledTask task(f, tid);
			if (task.fiber || task.cb)
			{
				tasks_.push(std::move(task));
			}
		}

		if (need_tickle)
		{
			tickle();
		}
	}

	ScheduledTask stealTask(size_t thief_id);

	virtual void init();
	virtual void stop();

  protected:
	void setLocalStance();

	virtual void tickle();
	virtual void run();
	virtual void idle();

	virtual bool stopping()
	{
		std::lock_guard<std::mutex> lock(mtx_);
		return stop_ && tasks_.empty() && (active_thread_count_ == 0);
	}

	bool hasIdleThread() const
	{
		return idle_thread_count_ > 0;
	}

  public:
	static Scheduler* localStance();
};
} // namespace koro

#endif