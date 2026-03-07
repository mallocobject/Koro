#include "koro/scheduler.h"
#include "elog/logger.h"
#include "koro/fiber.h"
#include "koro/task.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

using namespace koro;

Scheduler::Scheduler(size_t thread_count)
{
	assert(thread_count > 0);

	threads_.resize(thread_count);
	data_.resize(thread_count);

	LOG_DEBUG << "creat scheduler successfully";
}

Scheduler::~Scheduler()
{
	assert(stop_.load(std::memory_order_acquire));

	LOG_DEBUG << "destroy scheduler successfully";
}

void Scheduler::init()
{
	{
		std::lock_guard<std::mutex> lock(mtx_);
		stop_.store(false, std::memory_order_release);
		for (auto& data : data_)
		{
			data = std::make_unique<Data>();
		}

		for (size_t i = 0; i < threads_.size(); i++)
		{
			threads_[i] = std::jthread(std::bind(&Scheduler::run, this, i));
		}
	}

	LOG_DEBUG << "initialize scheduler successfully with " << threads_.size()
			  << " threads";
}

void Scheduler::stop()
{
	if (stop_.exchange(true, std::memory_order_acq_rel))
	{
		return;
	}

	for (size_t i = 0; i < threads_.size(); ++i)
	{
		tickle(i);
	}

	// while (!stopping())
	// {
	// 	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	// 	for (size_t i = 0; i < threads_.size(); ++i)
	// 	{
	// 		tickle(i);
	// 	}
	// }

	for (auto& thread : threads_)
	{
		thread.join();
	}

	LOG_DEBUG << "scheduler ends";
}

bool Scheduler::stopping(size_t skip_idx)
{
	if (!stop_.load(std::memory_order_acquire) ||
		active_thread_count_.load(std::memory_order_acquire) ||
		pending_event_count_.load(std::memory_order_acquire))
	{
		return false;
	}

	for (size_t i = 0; i < data_.size(); ++i)
	{
		if (i == skip_idx)
		{
			continue;
		}
		std::lock_guard<std::mutex> lock(data_[i]->mtx);
		if (!data_[i]->tasks.empty())
		{
			return false;
		}
	}

	return true;
}

void Scheduler::tickle(size_t idx)
{
	if (data_[idx]->idling.load(std::memory_order_acquire))
	{
		data_[idx]->cv.notify_one();
	}
}

void Scheduler::idle(size_t idx)
{
	Data& data = *data_[idx];
	while (true)
	{
		data.idling.store(true, std::memory_order_release);
		{
			std::unique_lock<std::mutex> lock(data.mtx);
			// data.cv.wait_for(lock, std::chrono::milliseconds(10));
			data.cv.wait(lock,
						 [&] {
							 return !data.tasks.empty() ||
									stop_.load(std::memory_order_acquire);
						 });
		}
		data.idling.store(false, std::memory_order_release);

		if (stopping())
		{
			break;
		}

		Fiber::runningFiber()->yield();
	}
}

void Scheduler::run(size_t thread_index)
{
	LOG_DEBUG << "sub scheduler runs in No. " << thread_index << " thread";

	Fiber::runningFiber(); // initialize main fiber, it cannot yield
	auto idle_fiber = std::make_shared<Fiber>(
		std::bind(&Scheduler::idle, this, thread_index));

	Data& data = *data_[thread_index];
	ScheduledTask task;

	while (true)
	{
		task.clear();
		bool has_task = false;

		{
			std::unique_lock<std::mutex> lock(data.mtx);
			if (!data.tasks.empty())
			{
				task = std::move(data.tasks.front());
				data.tasks.pop_front();
				has_task = true;
			}
		}

		if (!has_task)
		{
			task = stealTask(thread_index);
			if (task.fiber || task.cb)
			{
				has_task = true;
			}
		}

		if (!has_task)
		{
			if (idle_fiber->state() != Fiber::State::kTerm)
			{
				idle_fiber->resume();
				continue;
			}
			break;
		}

		assert(task.fiber || task.cb);
		active_thread_count_.fetch_add(1, std::memory_order_release);

		if (task.fiber)
		{

			task.fiber->resume();
			assert(task.fiber->state() != Fiber::State::kRunning);

			if (task.fiber->state() == Fiber::State::kReady)
			{
				std::lock_guard<std::mutex> q_lock(data.mtx);
				data.tasks.push_back(std::move(task));
			}
		}
		else
		{
			auto fiber_wrapper = std::make_shared<Fiber>(std::move(task.cb));
			fiber_wrapper->resume();
		}

		active_thread_count_.fetch_sub(1, std::memory_order_acquire);
	}

	cv_.notify_all();
}

ScheduledTask Scheduler::stealTask(size_t thief)
{
	ScheduledTask task;
	for (size_t i = 1; i < data_.size(); i++)
	{
		size_t victim = (thief + i) % data_.size();
		Data& data = *data_[victim];
		std::unique_lock<std::mutex> lock(data.mtx, std::try_to_lock);
		if (lock.owns_lock() && !data.tasks.empty())
		{
			task = std::move(data.tasks.back());
			data.tasks.pop_back();
			return task;
		}
	}
	return {};
}