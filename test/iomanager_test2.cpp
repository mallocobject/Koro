#include "elog/logger.h"
#include "koro/current_thread.h"
#include "koro/io_manager.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <sys/eventfd.h>
#include <thread>
#include <unistd.h>

std::atomic<int> g_task_count{0};

using namespace koro;

// 同步风格的异步IO任务（核心示例）
void sync_style_async_task()
{
	LOG_INFO << "测试开始";
	IOManager* iom = IOManager::localStance();
	int efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);

	// 启动一个辅助线程/协程去触发事件（模拟外部IO到来）
	std::thread(
		[efd]()
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
			uint64_t val = 1;
			LOG_INFO << "写入事件，唤醒协程";
			::write(efd, &val, sizeof(val));
		})
		.detach();

	LOG_INFO << "注册异步任务并挂起（Yield）";

	// 关键：传入 nullptr，触发 yield()
	iom->addEvent(efd, koro::IOManager::Event::kRead, nullptr);

	// 只有当 eventfd 可读后，协程被恢复，才会执行到这里
	LOG_INFO << "协程恢复，立即返回";

	uint64_t val = 0;
	LOG_INFO << "读出事件";
	g_task_count++;
	int ret = ::read(efd, &val, sizeof(val));
	LOG_INFO << "Read result: " << ret << ", val: " << val;
}

// 测试主函数
int main()
{
	// 初始化IOManager（2个工作线程）
	IOManager iom(2);

	// 提交10个同步风格的异步任务
	for (int i = 0; i < 1; ++i)
	{
		iom.submit(sync_style_async_task);
	}

	// 等待所有任务完成（同步等待，不阻塞线程）
	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	iom.stop();

	// 验证结果
	assert(g_task_count == 1);
	LOG_INFO << "所有异步任务执行完成，总数: "
			 << g_task_count.load(std::memory_order_relaxed);
	return 0;
}