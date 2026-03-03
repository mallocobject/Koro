#include "koro/semaphore.h"

using namespace koro;

Semaphore::Semaphore(size_t count) : count_(count)
{
}

void Semaphore::post()
{
	{
		std::lock_guard<std::mutex> lock(mtx_);
		count_++;
	}
	cv_.notify_one();
}

void Semaphore::wait()
{
	std::unique_lock<std::mutex> lock(mtx_);
	cv_.wait(lock, [this] { return count_ > 0; });
	count_--;
}