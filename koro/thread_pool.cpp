#include "koro/thread_pool.h"

using namespace koro;

ThreadPool::ThreadPool(size_t threads)
{
	workers_.reserve(threads);
	data_.reserve(threads);

	for (size_t i = 0; i < threads; i++)
	{
		data_.emplace_back(std::make_unique<WorkerData>());
	}

	for (size_t i = 0; i < threads; i++)
	{
		workers_.emplace_back(
			[this, i]()
			{
				auto& data = *data_[i];
				while (true)
				{
					InplaceFunction task;
					{
						std::unique_lock<std::mutex> lock(data.mtx);
						if (!data.queue.empty())
						{
							task = std::move(data.queue.front());
							data.queue.pop_front();
						}
						else if (!data.stop)
						{
							lock.unlock();
							task = stealTask(i);
							if (!task)
							{
								lock.lock();
								if (data.queue.empty() && !data.stop)
								{
									data.cv.wait(
										lock,
										[&data] {
											return !data.queue.empty() ||
												   data.stop;
										});
								}
								continue;
							}
						}
						else
						{
							return; //  step out
						}
					}
					task();
				}
			});
	}
}

ThreadPool::~ThreadPool()
{
	for (auto& data : data_)
	{
		{
			std::lock_guard<std::mutex> lock(data->mtx);
			data->stop = true;
		}
		data->cv.notify_one();
	}

	for (auto& worker : workers_)
	{
		worker.join();
	}
}

InplaceFunction<> ThreadPool::stealTask(size_t thief_id)
{
	for (size_t i = 1; i < data_.size(); i++)
	{
		size_t victim = (thief_id + i) % data_.size();
		auto& data = *data_[victim];
		std::unique_lock<std::mutex> lock(data.mtx, std::try_to_lock);
		if (lock.owns_lock() && !data.queue.empty())
		{
			InplaceFunction task = std::move(data.queue.back());
			data.queue.pop_back();
			return task;
		}
	}
	return InplaceFunction<>();
}