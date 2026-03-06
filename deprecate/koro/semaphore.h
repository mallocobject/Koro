#ifndef KORO_SEMAPHORE_H
#define KORO_SEMAPHORE_H

#include "koro/noncopyable.h"
#include <condition_variable>
#include <cstddef>
#include <mutex>
namespace koro
{
class Semaphore : public noncopyable
{
  private:
	size_t count_{0};
	std::mutex mtx_;
	std::condition_variable cv_;

  public:
	explicit Semaphore(size_t count = 0);
	void post();
	void wait();
};
} // namespace koro

#endif