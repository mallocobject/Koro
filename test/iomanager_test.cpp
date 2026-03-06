#include "elog/logger.h"
#include "koro/current_thread.h"
#include "koro/fiber.h"
#include "koro/io_manager.h"
#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <sys/eventfd.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace koro;

// 全局计数器（用于验证任务执行）
std::atomic<int> g_task_count{0};
std::atomic<int> g_io_event_count{0};

// 测试1：基础任务提交
void test_basic_task()
{
	LOG_INFO << "=== 测试基础任务提交 ===";
	IOManager iom(2); // 2个线程的IO调度器

	// 提交10个普通任务
	for (int i = 0; i < 10; ++i)
	{
		iom.submit(
			[]
			{
				LOG_DEBUG << "执行普通任务，线程ID: " << CurrentThread::tid();
				g_task_count.fetch_add(1, std::memory_order_relaxed);
				// 模拟任务耗时
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			});
	}

	// 等待任务执行完成
	std::this_thread::sleep_for(std::chrono::seconds(1));
	iom.stop();

	assert(g_task_count == 10);
	LOG_INFO << "基础任务提交测试通过，执行任务数: "
			 << g_task_count.load(std::memory_order_relaxed);
	g_task_count = 0;
}

// 测试2：IO事件（eventfd 读事件）
void test_io_event()
{
	LOG_INFO << "=== 测试IO事件（eventfd） ===";
	IOManager iom(1); // 1个线程的IO调度器

	// 创建eventfd（用于模拟IO事件）
	int efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	assert(efd >= 0);

	// 注册读事件
	bool add_ok = iom.addEvent(
		efd, IOManager::Event::kRead,
		[efd]
		{
			LOG_DEBUG << "触发读事件，线程ID: " << CurrentThread::tid();
			// 读取eventfd数据
			uint64_t val = 0;
			ssize_t n = read(efd, &val, sizeof(val));
			assert(n == sizeof(val));
			g_io_event_count.fetch_add(1, std::memory_order_relaxed);
		});
	assert(add_ok);

	// 往eventfd写数据，触发读事件
	uint64_t val = 100;
	ssize_t n = write(efd, &val, sizeof(val));
	assert(n == sizeof(val));

	// 等待IO事件处理完成
	std::this_thread::sleep_for(std::chrono::seconds(1));
	iom.stop();
	close(efd);

	assert(g_io_event_count == 1);
	LOG_INFO << "IO事件测试通过，触发事件数: "
			 << g_io_event_count.load(std::memory_order_relaxed);
	g_io_event_count = 0;
}

// 测试3：取消IO事件
void test_cancel_io_event()
{
	LOG_INFO << "=== 测试取消IO事件 ===";
	IOManager iom(1);

	int efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	assert(efd >= 0);

	// 注册读事件
	bool add_ok = iom.addEvent(efd, IOManager::Event::kRead,
							   []()
							   {
								   LOG_ERROR << "不应该执行这个回调！";
								   g_io_event_count.fetch_add(
									   1, std::memory_order_relaxed);
							   });
	assert(add_ok);

	// 取消读事件
	bool cancel_ok = iom.cancelEvent(efd, IOManager::Event::kRead);
	assert(cancel_ok);

	// 往eventfd写数据（此时事件已取消，不会触发回调）
	uint64_t val = 100;
	write(efd, &val, sizeof(val));

	std::this_thread::sleep_for(std::chrono::seconds(1));
	iom.stop();
	close(efd);

	assert(g_io_event_count == 0);
	LOG_INFO << "取消IO事件测试通过";
}

// 测试4：协程+IO事件（核心场景）
void test_fiber_io()
{
	LOG_INFO << "=== 测试协程+IO事件 ===";
	IOManager iom(2);

	// 提交协程任务，内部注册IO事件
	iom.submit(
		[]
		{
			LOG_DEBUG << "协程开始执行，线程ID: " << CurrentThread::tid();
			IOManager* cur_iom = IOManager::localStance();
			assert(cur_iom != nullptr);

			int efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
			assert(efd >= 0);

			// 注册写事件（当前协程会挂起，直到事件触发）
			bool add_ok =
				cur_iom->addEvent(efd, IOManager::Event::kWrite, [] {});
			assert(add_ok);

			// 此时协程会yield，直到写事件触发
			LOG_DEBUG << "协程挂起，等待写事件...";

			// 触发写事件（往eventfd写数据，实际写事件只要fd可写就触发）
			uint64_t val = 1;
			write(efd, &val, sizeof(val));

			// 等待事件处理完成
			std::this_thread::sleep_for(std::chrono::milliseconds(100));

			// 取消事件并关闭fd
			cur_iom->cancelEventAfterDone(efd);
			close(efd);

			LOG_DEBUG << "协程恢复执行完成";
			g_task_count.fetch_add(1, std::memory_order_relaxed);
		});

	std::this_thread::sleep_for(std::chrono::seconds(1));
	iom.stop();

	assert(g_task_count == 1);
	LOG_INFO << "协程+IO事件测试通过";
}

int main()
{

	try
	{
		test_basic_task();
		test_io_event();
		test_cancel_io_event();
		test_fiber_io();

		LOG_INFO << "所有测试用例执行通过！";
	}
	catch (const std::exception& e)
	{
		LOG_FATAL << "测试失败：" << e.what();
		return -1;
	}

	return 0;
}