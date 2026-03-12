#ifndef KORO_SCHEDULER_H
#define KORO_SCHEDULER_H

#include "koro/noncopyable.h"
#include "koro/task.h"
#include <atomic>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>
namespace koro
{
extern thread_local TaskQueue* t_task_queue;

class Channel;
class Fiber;
class Scheduler : public noncopyable
{
	using function = std::function<void()>;

  protected:
	std::atomic<bool> stop_{true};
	std::atomic<size_t> pending_event_count_{0};
	std::vector<std::unique_ptr<TaskQueue>> task_queues_;
	std::vector<std::jthread> threads_;
	std::atomic<size_t> active_thread_count_{0};
	std::atomic<size_t> thread_to_post_index_{0};

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
	virtual void onInit();

	void run(size_t thread_index);

	virtual void idle(size_t idx);

	virtual bool stopping(size_t skip_idx = std::numeric_limits<size_t>::max());

	// 通知线程唤醒 (tickle)
	virtual void tickle(size_t idx);

	std::shared_ptr<ScheduledTask> stealTask(size_t thief);
};

template <typename CF>
	requires std::is_same_v<std::decay_t<CF>, std::shared_ptr<Fiber>> ||
			 std::invocable<CF> && std::same_as<std::invoke_result_t<CF>, void>
			 void Scheduler::submit(CF&& cf)
{
	init();

	bool need_tickle = false;

	// fetch_add return old val
	// xxx algorithm
	size_t idx = thread_to_post_index_.fetch_add(1, std::memory_order_relaxed) %
				 threads_.size();
	TaskQueue& task_queue = *task_queues_[idx];
	{
		std::lock_guard<std::mutex> lock(task_queue.mtx);
		need_tickle = task_queue.tasks.empty();
		auto task = std::make_shared<ScheduledTask>(std::forward<CF>(cf));
		if (task)
		{
			task_queue.tasks.push_back(task);
		}
	}

	if (need_tickle)
	{
		tickle(idx);
	}
}

} // namespace koro

#endif