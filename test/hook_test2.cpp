#include "elog/logger.h"
#include "koro/hook.h"
#include "koro/io_manager.h"
#include <strings.h>

using namespace koro;

int main()
{
	const int numEvents = 50000;
	std::atomic<int> eventsHandled{0};
	std::vector<int> readFds(numEvents);
	std::vector<int> writeFds(numEvents);
	std::vector<Channel*> channels(numEvents, nullptr);

	for (int i = 0; i < numEvents; i++)
	{
		int fds[2];
		if (::pipe(fds) == -1)
		{
			LOG_ERROR << "pipe failed";
			return 1;
		}
		readFds[i] = fds[0];
		writeFds[i] = fds[1];

		auto task1 = [readFd = readFds[i], i, &eventsHandled]
		{
			char buf[1024];
			int n = read(readFd, buf, sizeof(buf));
			if (n > 0)
			{
				// LOG_FATAL << "received: " << std::string(buf, n)
				// 		  << " from pipe: " << i;
				++eventsHandled;
			}
			// close(readFd);
		};
		iom.submit(task1);
	}

	for (int i = 0; i < numEvents; i++)
	{
		auto task2 = [writeFd = writeFds[i]]
		{
			char buf[] = "Hello World";
			int n = write(writeFd, buf, sizeof(buf));
			// close(writeFd);
		};
		iom.submit(task2);
	}

	iom.stop();

	assert(eventsHandled.load() == numEvents);
	LOG_INFO << "events handled count: " << eventsHandled.load();

	// std::this_thread::sleep_for(std::chrono::milliseconds(10000));

	return 0;
}