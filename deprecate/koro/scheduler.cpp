#include "koro/scheduler.h"
#include "elog/logger.h"
#include "koro/current_thread.h"
#include "koro/fiber.h"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

using namespace koro;

thread_local Scheduler* t_scheduler = nullptr;
thread_local size_t koro::t_thread_idx = -1;

Scheduler::Scheduler(size_t thread_count)
	: thread_to_post_index_(0), active_thread_count_(0), stop_(false)
{
	assert(thread_count > 0 && !localStance());
	setLocalStance();

	threads_.resize(thread_count);
	data_.resize(thread_count);

	LOG_DEBUG << "create scheduler successfully";
}

Scheduler::~Scheduler()
{
	assert(stop_);
	if (localStance() == this)
	{
		t_scheduler = nullptr;
	}

	LOG_DEBUG << "destroy scheduler successfully";
}

void Scheduler::init()
{
	{
		std::lock_guard<std::mutex> lock(mtx_);
		if (stop_.load(std::memory_order_relaxed))
		{
			LOG_ERROR << "scheduler is stopped";
			return;
		}

		for (size_t i = 0; i < threads_.size(); i++)
		{
			data_[i] = std::make_unique<Data>();
		}

		for (size_t i = 0; i < threads_.size(); i++)
		{
			threads_[i] = std::thread(std::bind(&Scheduler::run, this, i));
		}
	}

	LOG_DEBUG << "initialize scheduler successfully with " << threads_.size()
			  << " threads";
}

// blocking, return only if all tasks are finished
void Scheduler::stop()
{
	if (stop_.exchange(true, std::memory_order_acq_rel))
	{
		return;
	}

	for (const auto& data : data_)
	{
		data->cv.notify_all();
	}

	std::vector<std::thread> tmp;
	{
		std::lock_guard<std::mutex> lock(mtx_);
		tmp.swap(threads_);
	}

	for (auto& t : tmp)
	{
		t.join();
	}

	LOG_DEBUG << "scheduler ends";
}

void Scheduler::setLocalStance()
{
	t_scheduler = this;
}

void Scheduler::tickle()
{
}

void Scheduler::run(size_t thread_index)
{
	t_thread_idx = thread_index;

	uint64_t tid = CurrentThread::tid();
	LOG_DEBUG << "scheduler runs in thread: " << tid;

	setLocalStance();

	Fiber::curFiberPtr();
	auto idle_fiber = std::make_shared<Fiber>([this] { this->idle(); });

	Data& data = *data_[thread_index];
	ScheduledTask task;

	while (!stopping())
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
				// 进入空闲，执行 idle 协程
				idle_fiber->resume(); // idle 内部会 yield 回来
			}
			continue; // 从 idle 返回后重新检查任务
		}

		active_thread_count_.fetch_add(1, std::memory_order_acq_rel);

		assert(task.fiber || task.cb);

		if (task.fiber)
		{
			{
				// std::lock_guard<std::mutex> lock(task.fiber->mtx_);
				if (task.fiber->state() != Fiber::State::kTerm)
				{
					task.fiber->resume();
				}

				assert(task.fiber->state() != Fiber::State::kRunning);

				if (task.fiber->state() == Fiber::State::kReady)
				{
					std::lock_guard<std::mutex> q_lock(data.mtx);
					data.tasks.push_back(std::move(task));
				}
			}
		}
		else if (task.cb)
		{
			auto fiber_guard = std::make_shared<Fiber>(std::move(task.cb));
			{
				// std::lock_guard<std::mutex> lock(fiber_guard->mtx_);
				fiber_guard->resume();
			}
		}
		active_thread_count_.fetch_sub(1, std::memory_order_acq_rel);
	}
}

void Scheduler::idle()
{
	Data& data = *data_[t_thread_idx];
	{
		std::unique_lock<std::mutex> lock(data.mtx);
		data.cv.wait(lock,
					 [&] {
						 return stop_.load(std::memory_order_acquire) ||
								!data.tasks.empty();
					 });
	}

	Fiber::curFiberPtr()->yield();
}

bool Scheduler::stopping()
{
	if (!stop_.load(std::memory_order_acquire))
	{
		return false;
	}

	for (auto& d : data_)
	{
		std::lock_guard<std::mutex> lock(d->mtx);
		if (!d->tasks.empty())
		{
			return false;
		}
	}

	return active_thread_count_.load(std::memory_order_acquire) == 0;
}

Scheduler* Scheduler::localStance()
{
	return t_scheduler;
}

Scheduler::ScheduledTask Scheduler::stealTask(size_t thief_id)
{
	ScheduledTask task;
	for (size_t i = 1; i < data_.size(); i++)
	{
		size_t victim = (thief_id + i) % data_.size();
		auto& data = *data_[victim];
		std::unique_lock<std::mutex> lock(data.mtx, std::try_to_lock);
		if (lock.owns_lock() && !data.tasks.empty())
		{
			task = std::move(data.tasks.back());
			data.tasks.pop_back();
			return task;
		}
	}
	return task;
}

void Scheduler::wakeupThread(int idx)
{
	Data& data = *data_[idx];
	data.cv.notify_one();
}

void Scheduler::waitForEvent(int idx)
{
}