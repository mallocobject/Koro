#ifndef KORO_FIBER_H
#define KORO_FIBER_H

#include "koro/noncopyable.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <sys/ucontext.h>
namespace koro
{
class Fiber;
extern thread_local Fiber* t_thread_fiber;
// resume 由调度协程自动调用
// yield/hold 由任务协程主动调动
class Fiber : public noncopyable, public std::enable_shared_from_this<Fiber>
{
	using function = std::function<void()>;

  public:
	enum class State : uint8_t
	{
		kReady,
		kRunning,
		kHold,
		kTerm
	};

  private:
	State state_{State::kReady};
	function cb_;
	bool run_in_scheduler_{true};

	ucontext_t ctx_;
	uint32_t stack_size_{0};
	void* stack_sp_{nullptr};

  public:
	Fiber(function cb, bool run_in_scheduler = true,
		  uint32_t stack_size = 128 * 1024);
	~Fiber();

	void resetFunc(function cb);
	void resume(); // scheduler -> this
	void hold();   // yield duo to waiting event
	void yield();  // this -> scheduler

	State state() const
	{
		return state_;
	}

  private:
	Fiber();

  public:
	static void setSchedulerFiber(const std::shared_ptr<Fiber>& fiber);
	static std::shared_ptr<Fiber> runningFiber();

  private:
	static void setRunningFiber(Fiber* fiber);
	static void mainFunc();
};
} // namespace koro

#endif