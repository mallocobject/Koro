#include "koro/fiber.h"
#include "elog/logger.h"
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <ucontext.h>
#include <utility>

using namespace koro;

thread_local Fiber* t_fiber = nullptr;				   // temporary
thread_local std::shared_ptr<Fiber> t_scheduler_fiber; // prolong object life
thread_local Fiber* t_thread_fiber = nullptr;		   // safe

// for scheduled fiber
Fiber::Fiber() : state_(State::kRunning), run_in_scheduler_(false)
{
	setRunningFiber(this);

	if (::getcontext(&ctx_))
	{
		LOG_FATAL << "get context failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}

	ctx_.uc_link = nullptr;
	ctx_.uc_stack.ss_flags = 0;
	ctx_.uc_stack.ss_sp = stack_sp_;
	ctx_.uc_stack.ss_size = stack_size_;
}

// 调度协程 run_in_scheduler = false
Fiber::Fiber(function cb, bool run_in_scheduler, uint32_t stack_size)
	: cb_(std::move(cb)), stack_size_(stack_size),
	  run_in_scheduler_(run_in_scheduler)
{
	assert(stack_size != 0);
	stack_sp_ = ::malloc(stack_size);

	if (::getcontext(&ctx_))
	{
		LOG_FATAL << "get context failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}

	ctx_.uc_link = nullptr;
	ctx_.uc_stack.ss_flags = 0;
	ctx_.uc_stack.ss_sp = stack_sp_;
	ctx_.uc_stack.ss_size = stack_size_;

	::makecontext(&ctx_, &Fiber::mainFunc, 0);
}

Fiber::~Fiber()
{
	if (stack_sp_)
	{
		::free(stack_sp_);
	}
	else
	{
		assert(state_ == State::kRunning);
	}
}

void Fiber::resetFunc(function cb)
{
	assert(stack_sp_);

	state_ = State::kReady;
	cb_ = std::move(cb);

	if (::getcontext(&ctx_))
	{
		LOG_FATAL << "get context failed: " << ::strerror(errno);
		::exit(EXIT_FAILURE);
	}

	ctx_.uc_link = nullptr;
	ctx_.uc_stack.ss_flags = 0;
	ctx_.uc_stack.ss_sp = stack_sp_;
	ctx_.uc_stack.ss_size = stack_size_;

	::makecontext(&ctx_, &Fiber::mainFunc, 0);
}

void Fiber::resume()
{
	assert(state_ == State::kReady);
	state_ = State::kRunning;

	setRunningFiber(this);
	if (run_in_scheduler_)
	{
		if (::swapcontext(&t_scheduler_fiber->ctx_, &ctx_))
		{
			LOG_FATAL << "resume <t_scheduled_fiber> failed";
			::exit(EXIT_FAILURE);
		}
	}
	else
	{
		if (::swapcontext(&t_thread_fiber->ctx_, &ctx_))
		{
			LOG_FATAL << "resume <t_thread_fiber> failed";
			::exit(EXIT_FAILURE);
		}
	}
}

void Fiber::yield()
{
	assert(state_ == State::kRunning || state_ == State::kTerm);
	if (state_ != State::kTerm)
	{
		state_ = State::kReady;
	}

	if (run_in_scheduler_)
	{
		setRunningFiber(t_scheduler_fiber.get());
		if (::swapcontext(&ctx_, &t_scheduler_fiber->ctx_))
		{
			LOG_FATAL << "yield <t_scheduled_fiber> failed";
			::exit(EXIT_FAILURE);
		}
	}
	else
	{
		setRunningFiber(t_thread_fiber);
		if (::swapcontext(&ctx_, &t_thread_fiber->ctx_))
		{
			LOG_FATAL << "yield <t_thread_fiber> failed";
			::exit(EXIT_FAILURE);
		}
	}
}

void Fiber::setRunningFiber(Fiber* fiber)
{
	t_fiber = fiber;
}

// 只准任务协程使用，t_scheduler_fiber会覆盖
void Fiber::setSchedulerFiber(const std::shared_ptr<Fiber>& fiber)
{
	t_scheduler_fiber = fiber;
}

std::shared_ptr<Fiber> Fiber::runningFiber()
{
	if (t_fiber)
	{
		return t_fiber->shared_from_this();
	}

	std::shared_ptr<Fiber> fiber(
		new Fiber); // 默认构造必须设置t_fiber，否则会多次创建fiber，为后续调度协程和idle协程提供合法当前指针
	t_thread_fiber = &*fiber;
	t_scheduler_fiber = fiber;

	return fiber->shared_from_this();
}

void Fiber::mainFunc()
{
	std::shared_ptr<Fiber> running_fiber = runningFiber();
	assert(running_fiber != nullptr && running_fiber.get() != t_thread_fiber);

	if (running_fiber->cb_)
	{
		running_fiber->cb_();
	}
	running_fiber->cb_ = nullptr;
	running_fiber->state_ = State::kTerm;

	// LOG_DEBUG << "before yield";

	// cur->yield(); // co_return

	// 减少计数，外部持有共享指针，不会提前释放
	Fiber* raw_ptr = running_fiber.get();
	running_fiber.reset(); // cur == t_fiber
	raw_ptr->yield();	   // yield 后无法及时销毁局部变量
}