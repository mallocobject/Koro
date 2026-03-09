#ifndef KORO_THREAD_POOL_H
#define KORO_THREAD_POOL_H

#include "koro/inplace_function.hpp"
#include "koro/noncopyable.h"
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <vector>
namespace koro
{
class ThreadPool : public noncopyable
{
  private:
	struct WorkerData
	{
		std::deque<InplaceFunction<>> queue;
		std::mutex mtx;
		std::condition_variable cv;
		bool stop{false};
	};

	std::vector<std::thread> workers_;
	std::vector<std::unique_ptr<WorkerData>> data_;
	std::atomic<size_t> cur_id{0};

  public:
	explicit ThreadPool(size_t threads);
	~ThreadPool();

	template <typename F, typename... Args>
	auto submit(F&& f,
				Args&&... args) -> std::future<std::invoke_result_t<F, Args...>>
	{

		using return_type = std::invoke_result_t<F, Args...>;
		auto task = std::make_shared<std::packaged_task<return_type()>>(
			std::bind(std::forward<F>(f), std::forward<Args>(args)...));
		std::future<return_type> result = task->get_future();

		// fetch_add return old val
		// xxx algorithm
		auto& data = *data_[cur_id.fetch_add(1, std::memory_order_relaxed) %
							workers_.size()];
		{
			std::lock_guard<std::mutex> lock(data.mtx);
			data.queue.emplace_back([task] { (*task)(); });
		}
		data.cv.notify_one();
		return result;
	}

	InplaceFunction<> stealTask(size_t thief_id);
};
} // namespace koro

#endif