#include "koro/scheduler.h"
#include "elog/logger.h"
#include "koro/fiber.h"
#include "koro/task.h"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace koro
{
thread_local TaskQueue* t_task_queue = nullptr;
}

using namespace koro;

Scheduler::Scheduler(size_t thread_count)
{
	assert(thread_count > 0);

	threads_.resize(thread_count);
	task_queues_.resize(thread_count);

	LOG_TRACE << "creat scheduler successfully";
}

Scheduler::~Scheduler()
{
	assert(stop_.load(std::memory_order_relaxed));

	LOG_TRACE << "destroy scheduler successfully";
}

void Scheduler::onInit()
{
}

void Scheduler::init()
{
	static bool initialized = [this]
	{
		stop_.store(false, std::memory_order_relaxed);
		for (auto& task_queue : task_queues_)
		{
			task_queue = std::make_unique<TaskQueue>();
		}

		onInit();

		for (size_t i = 0; i < threads_.size(); i++)
		{
			threads_[i] = std::jthread(std::bind(&Scheduler::run, this, i));
		}

		LOG_DEBUG << "initialize scheduler successfully with "
				  << threads_.size() << " threads";

		return true;
	}();
}

void Scheduler::stop()
{
	if (stop_.exchange(true, std::memory_order_seq_cst))
	{
		return;
	}

	do
	{
		for (size_t i = 0; i < threads_.size(); ++i)
		{
			tickle(i);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	} while (!stopping());

	for (size_t i = 0; i < threads_.size(); ++i)
	{
		tickle(i);
	}

	LOG_TRACE << "start wait threads join";

	for (auto& thread : threads_)
	{
		thread.join();
	}

	LOG_TRACE << "scheduler ends";
}

bool Scheduler::stopping(size_t skip_idx)
{
	if (!stop_.load(std::memory_order_acquire) ||
		active_thread_count_.load(std::memory_order_acquire) ||
		pending_event_count_.load(std::memory_order_acquire))
	{
		return false;
	}

	for (size_t i = 0; i < task_queues_.size(); ++i)
	{
		if (i == skip_idx)
		{
			continue;
		}
		std::lock_guard<std::mutex> lock(task_queues_[i]->mtx);
		if (!task_queues_[i]->tasks.empty())
		{
			return false;
		}
	}

	return true;
}

void Scheduler::tickle(size_t idx)
{
	if (task_queues_[idx]->idling.load(std::memory_order_seq_cst))
	{
		task_queues_[idx]->cv.notify_one();
	}
}

void Scheduler::idle(size_t idx)
{
	TaskQueue& task_queue = *task_queues_[idx];
	while (true)
	{
		task_queue.idling.store(true, std::memory_order_seq_cst);
		{
			std::unique_lock<std::mutex> lock(task_queue.mtx);
			// task_queue.cv.wait_for(lock, std::chrono::milliseconds(10));
			task_queue.cv.wait(lock);
		}
		task_queue.idling.store(false, std::memory_order_release);

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

	t_task_queue = task_queues_[thread_index].get();
	std::shared_ptr<ScheduledTask> task;

	while (true)
	{
		bool has_task = false;

		{
			std::unique_lock<std::mutex> lock(t_task_queue->mtx);
			if (!t_task_queue->tasks.empty())
			{
				task = t_task_queue->tasks.front();
				t_task_queue->tasks.pop_front();
				if (task)
				{
					has_task = true;
				}
			}
		}

		if (!has_task)
		{
			task = stealTask(thread_index);
			if (task)
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

		assert(task);
		active_thread_count_.fetch_add(1, std::memory_order_acquire);

		if (task->fiber)
		{

			task->fiber->resume();
			assert(task->fiber->state() != Fiber::State::kRunning);

			if (task->fiber->state() == Fiber::State::kReady)
			{
				std::lock_guard<std::mutex> q_lock(t_task_queue->mtx);
				t_task_queue->tasks.push_back(task);
			}
		}
		else
		{
			auto fiber_wrapper = std::make_shared<Fiber>(std::move(task->cb));
			fiber_wrapper->resume();
		}

		active_thread_count_.fetch_sub(1, std::memory_order_release);
	}
}

std::shared_ptr<ScheduledTask> Scheduler::stealTask(size_t thief)
{
	std::shared_ptr<ScheduledTask> task;
	for (size_t i = 1; i < task_queues_.size(); i++)
	{
		size_t victim = (thief + i) % task_queues_.size();
		TaskQueue& task_queue = *task_queues_[victim];
		std::unique_lock<std::mutex> lock(task_queue.mtx, std::try_to_lock);
		if (lock.owns_lock() && !task_queue.tasks.empty())
		{
			task = task_queue.tasks.back();
			task_queue.tasks.pop_back();
			return task;
		}
	}
	return task;
}