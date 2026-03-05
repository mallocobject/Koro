#include "koro/fiber.h"
#include "elog/logger.h"
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <ucontext.h>
#include <utility>

using namespace koro;

thread_local Fiber* t_fiber = nullptr;			  // 执行协程（子协程）
thread_local std::shared_ptr<Fiber> t_main_fiber; // 主协程
thread_local Fiber* t_scheduled_fiber = nullptr;  // 调度协程

static std::atomic<uint64_t> s_fiber_id{1};
static std::atomic<uint64_t> s_fiber_count{0};

Fiber::Fiber(inplace_function cb, uint32_t stack_size, bool run_in_scheduler)
	: cb_(std::move(cb)), run_in_scheduler_(run_in_scheduler),
	  state_(State::kReady), stack_size_(stack_size)
{
	assert(stack_size != 0);
	stack_sp_ = malloc(stack_size);

	if (getcontext(&ctx_))
	{
		LOG_FATAL << "get context failed: " << strerror(errno);
		exit(EXIT_FAILURE);
	}

	ctx_.uc_link = nullptr;
	ctx_.uc_stack.ss_flags = 0;
	ctx_.uc_stack.ss_sp = stack_sp_;
	ctx_.uc_stack.ss_size = stack_size_;

	makecontext(&ctx_, &Fiber::mainFunc, 0);

	id_ = s_fiber_id.fetch_add(1, std::memory_order_acq_rel);
	s_fiber_count.fetch_add(1, std::memory_order_acq_rel);

	LOG_DEBUG << "child fiber id = " << id_;
}

Fiber::Fiber() : run_in_scheduler_(false), state_(State::kRunning)
{
	setCurFiber(this);

	if (getcontext(&ctx_))
	{
		LOG_FATAL << "get context failed: " << strerror(errno);
		exit(EXIT_FAILURE);
	}

	id_ = s_fiber_id.fetch_add(1, std::memory_order_acq_rel);
	s_fiber_count.fetch_add(1, std::memory_order_acq_rel);

	LOG_DEBUG << "main fiber id = " << id_;
}

Fiber::~Fiber()
{
	s_fiber_count--;
	if (stack_sp_)
	{
		assert(state_ == State::kTerm);
		free(stack_sp_);
	}
	else
	{
		// 没有栈，说明是主协程
		assert(state_ == State::kRunning);
		Fiber* cur = t_fiber;
		if (cur == this)
		{
			setCurFiber(nullptr);
		}
	}
}

void Fiber::clear(inplace_function cb)
{
	assert(stack_sp_ && state_ == State::kTerm);

	state_ = State::kReady;
	cb_ = std::move(cb);

	if (getcontext(&ctx_))
	{
		LOG_FATAL << "get context failed: " << strerror(errno);
		exit(EXIT_FAILURE);
	}

	ctx_.uc_link = nullptr;
	ctx_.uc_stack.ss_flags = 0;
	ctx_.uc_stack.ss_sp = stack_sp_;
	ctx_.uc_stack.ss_size = stack_size_;

	makecontext(&ctx_, &Fiber::mainFunc, 0);
}

void Fiber::resume()
{
	assert(state_ == State::kReady);
	state_ = State::kRunning;

	setCurFiber(this);
	if (run_in_scheduler_)
	{
		if (swapcontext(&t_scheduled_fiber->ctx_, &ctx_))
		{
			LOG_FATAL << "resume <t_scheduled_fiber> failed";
			exit(EXIT_FAILURE);
		}
	}
	else
	{
		if (swapcontext(&t_main_fiber->ctx_, &ctx_))
		{
			LOG_FATAL << "resume <t_main_fiber> failed";
			exit(EXIT_FAILURE);
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
		setCurFiber(t_scheduled_fiber);
		if (swapcontext(&ctx_, &t_scheduled_fiber->ctx_))
		{
			LOG_FATAL << "yield <t_scheduled_fiber> failed";
			exit(EXIT_FAILURE);
		}
	}
	else
	{
		setCurFiber(t_main_fiber.get());
		if (swapcontext(&ctx_, &t_main_fiber->ctx_))
		{
			LOG_FATAL << "yield <t_main_fiber> failed";
			exit(EXIT_FAILURE);
		}
	}
}

void Fiber::setCurFiber(Fiber* f)
{
	t_fiber = f;
}

std::shared_ptr<Fiber> Fiber::curFiberPtr()
{
	if (t_fiber)
	{
		return t_fiber->shared_from_this();
	}

	// std::shared_ptr<Fiber> main_fiber = std::make_shared<Fiber>();
	std::shared_ptr<Fiber> main_fiber(new Fiber);
	t_main_fiber = main_fiber;
	t_scheduled_fiber = main_fiber.get();

	assert(t_fiber == main_fiber.get());
	return t_fiber->shared_from_this();
}

void Fiber::setSchduledFiber(Fiber* f)
{
	t_scheduled_fiber = f;
}

uint64_t Fiber::curFiberId()
{
	if (t_fiber)
	{
		return t_fiber->id();
	}
	return 0;
}

void Fiber::mainFunc()
{
	std::shared_ptr<Fiber> cur = curFiberPtr();
	assert(cur != nullptr);

	if (cur->cb_)
	{
		cur->cb_();
	}
	cur->cb_ = nullptr;
	cur->state_ = State::kTerm;

	// LOG_DEBUG << "before yield";

	// cur->yield(); // co_return

	// 减少计数，外部持有共享指针，不会提前释放
	Fiber* raw_ptr = cur.get();
	cur.reset(); // cur == t_fiber
	raw_ptr->yield();

	// LOG_DEBUG << "after yield"; // never run this
}