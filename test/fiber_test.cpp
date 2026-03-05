#include "elog/logger.h"
#include "koro/fiber.h"
#include <memory>

using namespace koro;

int main()
{
	Fiber::curFiberPtr();

	auto f = std::make_shared<Fiber>(
		[]
		{
			LOG_INFO << "Hello world";
			Fiber::curFiberPtr()->yield();
		});

	f->resume();
}