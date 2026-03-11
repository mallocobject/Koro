#include "elog/logger.h"
#include "koro/channel.h"
#include "koro/io_manager.h"
#include "koro/task.h"
#include <cassert>
#include <memory>
#include <unistd.h>

using namespace koro;

int main()
{
	// 创建包含 4 个工作线程的 IOManager
	auto iom = std::make_unique<IOManager>(16);
	iom.init();

	const int numEvents = 100000;
	std::atomic<int> eventsHandled{0};
	std::vector<int> readFds(numEvents);
	std::vector<int> writeFds(numEvents);
	std::vector<Channel*> channels(numEvents, nullptr);

	// 创建 numEvents 个管道，并为每个读端注册事件
	for (int i = 0; i < numEvents; ++i)
	{
		int fds[2];
		if (::pipe(fds) == -1)
		{
			LOG_ERROR << "pipe failed";
			return 1;
		}
		readFds[i] = fds[0];
		writeFds[i] = fds[1];

		auto ch = iom.bindTaskQueue(readFds[i]);
		iom.registerEvent(
			ch, koro::IOManager::Event::kRead,
			std::make_shared<ScheduledTask>(
				[&iom, &eventsHandled, ch, readFd = readFds[i], i]()
				{
					char buf[32];
					ssize_t n = read(readFd, buf, sizeof(buf) - 1);
					if (n > 0)
					{
						buf[n] = '\0';
						LOG_FATAL << "Read from pipe " << i << ": " << buf;
					}
					iom.unregisterEvent(ch, IOManager::Event::kRead);
					++eventsHandled;
				}));
	}

	// 向每个管道写入数据，触发读事件
	for (int i = 0; i < numEvents; ++i)
	{
		std::string msg = "Hello from " + std::to_string(i);
		::write(writeFds[i], msg.c_str(), msg.size());
	}

	iom.stop();

	assert(eventsHandled.load() == numEvents);
	LOG_INFO << "events handled count: " << eventsHandled.load();

	// 关闭文件描述符
	for (int i = 0; i < numEvents; ++i)
	{
		::close(readFds[i]);
		::close(writeFds[i]);
	}

	LOG_INFO << "Test completed successfully.";
	return 0;
}
