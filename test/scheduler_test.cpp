#include "elog/logger.h"
#include "koro/scheduler.h"
#include <atomic>
#include <cmath>
#include <cstdint>
#include <functional>
#include <vector>

using namespace koro;

std::atomic<uint64_t> count = 0;

void compute(int id, int intensity, double* result)
{
	*result = 0.0;
	for (int i = 0; i < intensity; ++i)
	{
		*result += std::sin(i * 0.01) * std::cos(id * 0.01) + std::sqrt(i + id);
	}
	count.fetch_add(1, std::memory_order_acq_rel);
}

int main()
{

	size_t thread_count = 16;
	Scheduler* s = new Scheduler(thread_count);
	LOG_INFO << "thread count: " << thread_count;
	s->init();

	const int task_count = 1000;  // 任务总数
	const int intensity = 100000; // 单个任务的循环次数

	std::vector<double> results(task_count, 0.0);

	auto start_time = std::chrono::steady_clock::now();

	for (int i = 0; i < task_count; ++i)
	{
		s->submit(std::bind(&compute, i, intensity, &results[i]));
	}

	s->stop();

	double total_sum = 0;
	for (auto& r : results)
	{
		total_sum += r; // blocking
	}

	delete s;

	auto end_time = std::chrono::steady_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
						end_time - start_time)
						.count();

	LOG_INFO << "  - Total Duration:       " << duration << " ms";
	LOG_INFO << "  - Checksum:             " << total_sum
			 << " (Prevention of optimization)";
	LOG_INFO << "  - Total Tasks:          " << count.load();
}