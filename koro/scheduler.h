#ifndef KORO_SCHEDULER_H
#define KORO_SCHEDULER_H

#include "koro/noncopyable.h"
#include "koro/task.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <limits>
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
	using function = std::function<void()>;

  private:
	struct Data
	{
		std::deque<ScheduledTask> tasks;
		std::mutex mtx;
		std::condition_variable cv;
		std::atomic<bool> idling{false};
	};

  private:
	std::vector<std::unique_ptr<Data>> data_;
	std::vector<std::jthread> threads_;

	std::atomic<size_t> active_thread_count_{0};
	std::atomic<size_t> thread_to_post_index_{0};

	std::mutex mtx_;
	std::condition_variable cv_;
	std::atomic<size_t> pending_event_count_{0};

  protected:
	std::atomic<bool> stop_{true};

  public:
	explicit Scheduler(size_t thread_count = 1);
	virtual ~Scheduler();

	void init();
	void stop();

	template <typename CF>
		requires std::is_same_v<std::decay_t<CF>, std::shared_ptr<Fiber>> ||
				 std::invocable<CF> &&
					 std::same_as<std::invoke_result_t<CF>, void>
				 void submit(CF&& cf);

  protected:
	void run(size_t thread_index);

	// 默认实现：cv.wait_for
	// IOManager实现：epoll_wait（最近定时器时间）
	virtual void idle(size_t idz);

	virtual bool stopping(size_t skip_idx = std::numeric_limits<size_t>::max());

	// 通知线程唤醒 (tickle)
	virtual void tickle(size_t idx);

	ScheduledTask stealTask(size_t thief);
};

template <typename CF>
	requires std::is_same_v<std::decay_t<CF>, std::shared_ptr<Fiber>> ||
			 std::invocable<CF> && std::same_as<std::invoke_result_t<CF>, void>
			 void Scheduler::submit(CF&& cf)
{
	bool need_tickle = false;

	// fetch_add return old val
	// xxx algorithm
	size_t idx = thread_to_post_index_.fetch_add(1, std::memory_order_acq_rel) %
				 threads_.size();
	Data& data = *data_[idx];
	{
		std::lock_guard<std::mutex> lock(data.mtx);
		need_tickle = data.tasks.empty();
		ScheduledTask task(std::forward<CF>(cf));
		if (task.fiber || task.cb)
		{
			data.tasks.push_back(std::move(task));
		}
	}

	if (need_tickle)
	{
		tickle(idx);
	}
}

} // namespace koro

#endif