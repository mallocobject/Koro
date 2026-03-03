#ifndef KORO_SCHEDULER_H
#define KORO_SCHEDULER_H

#include "koro/noncopyable.h"
#include <atomic>
#include <cstddef>
#include <functional>
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
  private:
	struct ScheduledTask
	{
		std::shared_ptr<Fiber> fiber;
		std::function<void()> cb;
		std::thread::id tid;

		ScheduledTask()
		{
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

  private:
	std::mutex mtx_;
	std::vector<std::shared_ptr<std::thread>> threads_;
	std::vector<ScheduledTask> tasks_;

	size_t thread_count_{0};
	std::atomic<size_t> active_thread_count_{0};
	std::atomic<size_t> idle_thread_count_{0};

	bool use_caller_{true};
	std::shared_ptr<Fiber> main_scheduled_fiber_;
	std::thread::id main_thread_id_;

	bool stop_{false};

  public:
	Scheduler(size_t thread_num = 1, bool use_caller = true);
	virtual ~Scheduler();

	template <typename F>
		requires std::is_same_v<std::decay_t<F>, std::shared_ptr<Fiber>> ||
				 std::is_same_v<std::decay_t<F>, std::function<void()>>
	void postTask(F f, std::thread::id tid)
	{
		bool need_tickle = false;
		{
			std::lock_guard<std::mutex> lock(mtx_);
			need_tickle = tasks_.empty();
			ScheduledTask task(f, tid);
			if (task.fiber || task.cb)
			{
				tasks_.push_back(task);
			}
		}

		if (need_tickle)
		{
			tickle();
		}
	}

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