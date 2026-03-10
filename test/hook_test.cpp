#include "elog/logger.h"
#include "koro/hook.h"
#include <strings.h>

using namespace koro;

int main()
{
	int fds[2];
	::pipe(fds);
	int readFd = fds[0];
	int writeFd = fds[1];

	auto task1 = [readFd]
	{
		char buf[1024];
		int n = read(readFd, buf, sizeof(buf));
		if (n > 0)
		{
			LOG_INFO << "received: " << std::string(buf, n);
		}
	};
	iom->submit(task1);

	LOG_INFO << "before write";
	auto task2 = [writeFd]
	{
		char buf[] = "Hello World";
		int n = write(writeFd, buf, sizeof(buf));
		LOG_INFO << "send: " << buf;
	};
	iom->submit(task2);
	LOG_INFO << "after write";
	// std::this_thread::sleep_for(std::chrono::milliseconds(100));

	return 0;
}