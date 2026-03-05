#include "elog/logger.h"
#include "koro/thread_pool.h"
#include <chrono>
#include <cmath>
#include <thread>

using namespace koro;

double compute(int id, int intensity)
{
	double result = 0.0;
	for (int i = 0; i < intensity; ++i)
	{
		result += std::sin(i * 0.01) * std::cos(id * 0.01) + std::sqrt(i + id);
	}
	return result;
}

int main()
{
	size_t pool_size = std::thread::hardware_concurrency();

	ThreadPool tp(pool_size);

	LOG_INFO << "Thread Pool Size:     " << pool_size;

	const int task_count = 1000;  // 任务总数
	const int intensity = 100000; // 单个任务的循环次数

	std::vector<std::future<double>> futures;
	futures.reserve(task_count);

	auto start_time = std::chrono::steady_clock::now();

	for (int i = 0; i < task_count; ++i)
	{
		futures.emplace_back(tp.submit(compute, i, intensity));
	}

	double total_sum = 0;
	for (auto& f : futures)
	{
		total_sum += f.get(); // blocking
	}

	auto end_time = std::chrono::steady_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
						end_time - start_time)
						.count();

	LOG_INFO << "  - Total Duration:       " << duration << " ms";
	LOG_INFO << "  - Checksum:             " << total_sum
			 << " (Prevention of optimization)";

	return 0;
}