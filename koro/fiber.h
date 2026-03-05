#ifndef KORO_FIBER_H
#define KORO_FIBER_H

#include "koro/inplace_function.hpp"
#include "koro/noncopyable.h"
#include <cstdint>
#include <memory>
#include <mutex>
#include <sys/ucontext.h>
namespace koro
{
static const uint32_t STACK_SIZE = 128 * 1024; // 128

class Fiber : public noncopyable, public std::enable_shared_from_this<Fiber>
{
	using inplace_function = InplaceFunction<64>;
	friend std::shared_ptr<Fiber> std::make_shared<Fiber>();

  public:
	enum class State : uint8_t
	{
		kReady,	  // 可（继续）执行
		kRunning, // 正在执行
		kTerm	  // 执行完成
	};

  private:
	State state_{Fiber::State::kReady};
	uint64_t id_{0};
	bool run_in_scheduler_{true};
	inplace_function cb_;

	ucontext_t ctx_;
	uint32_t stack_size_{0};
	void* stack_sp_{nullptr};

  public:
	std::mutex mtx_;

  public:
	// for creating scheduled fiber or child fiber
	Fiber(inplace_function cb, uint32_t stack_size = 128 * 1024,
		  bool run_in_scheduler = true);
	~Fiber();

	void clear(inplace_function cb);
	void resume();
	void yield();
	uint64_t id() const
	{
		return id_;
	}
	State state() const
	{
		return state_;
	}

  private:
	Fiber(); // for creating main coroutine

  public:
	static void setCurFiber(Fiber* f);
	static std::shared_ptr<Fiber> curFiberPtr();
	static void setSchduledFiber(Fiber* f);
	static uint64_t curFiberId();
	static void mainFunc();
};
} // namespace koro

#endif