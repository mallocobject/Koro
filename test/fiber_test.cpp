#include "elog/logger.h"
#include "koro/fiber.h"
#include <memory>
#include <thread>

using namespace koro;

void test1()
{
	LOG_INFO << "test starts";

	// 初始化线程主协程
	Fiber::runningFiber();
	// 创建调度协程
	auto fiber = std::make_shared<Fiber>(
		[]
		{
			LOG_INFO << "task fiber starts";
			// 创建任务协程
			auto fiber2 = std::make_shared<Fiber>(
				[]
				{
					LOG_INFO << "task fiber2 starts";
					Fiber::runningFiber()->yield();
					LOG_INFO << "task fiber2 resumes (should not reach here)";
				});
			fiber2->setSchedulerFiber(Fiber::runningFiber());
			fiber2->resume();
			LOG_INFO << "back to task fiber after fiber2 yielded";
			Fiber::runningFiber()->yield();
			LOG_INFO << "task fiber resumes again (after second resume)";
			// fiber2->resume();
		},
		false);

	LOG_INFO << "fiber created, now resume it";
	fiber->resume(); // 第一次 resume，进入 fiber
	LOG_INFO << "after first yield of fiber";
	fiber->resume(); // 第二次 resume，继续 fiber 剩余部分
	LOG_INFO << "test ends";
}

int main()
{
	std::thread(test1).join();
}