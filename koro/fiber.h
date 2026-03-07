#ifndef KORO_FIBER_H
#define KORO_FIBER_H

#include "koro/noncopyable.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <sys/ucontext.h>
namespace koro
{
// resume 只能在外部使用
// yield 只能在内部使用（在回调中使用）
class Fiber : public noncopyable, public std::enable_shared_from_this<Fiber>
{
	using function = std::function<void()>;

  public:
	enum class State : uint8_t
	{
		kReady,
		kRunning,
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