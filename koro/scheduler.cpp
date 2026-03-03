#include "koro/scheduler.h"
#include "elog/logger.h"
#include "koro/fiber.h"
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

using namespace koro;

thread_local Scheduler* t_scheduler = nullptr;

Scheduler::Scheduler(size_t thread_count, bool use_caller)
	: active_thread_count_(0), idle_thread_count_(0), use_caller_(use_caller),
	  stop_(false), thread_count_(thread_count)
{
	assert(thread_count_ > 0 && !localStance());
	setLocalStance();

	if (use_caller)
	{
		thread_count_--;
		localStance();

		main_scheduled_fiber_ =
			std::make_shared<Fiber>(std::bind(&Scheduler::run, this), 0, false);
		Fiber::setSchduledFiber(main_scheduled_fiber_.get());
		main_thread_id_ = std::this_thread::get_id();
	}

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
		if (stop_)
		{
			LOG_ERROR << "scheduler is stopped";
			return;
		}

		assert(threads_.empty());
		threads_.reserve(thread_count_);
		for (size_t i = 0; i < thread_count_; i++)
		{
			auto thread =
				std::make_shared<std::thread>(std::bind(&Scheduler::run, this));
			threads_.push_back(thread);
		}
	}

	LOG_DEBUG << "initialize scheduler successfully";
}

void Scheduler::stop()
{
}

void Scheduler::setLocalStance();

void Scheduler::tickle();
void Scheduler::run();
void Scheduler::idle();

Scheduler* Scheduler::localStance();