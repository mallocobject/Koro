#include "elog/logger.h"
#include "koro/channel.h"
#include "koro/hook.h"
#include "koro/io_manager.h"
#include "koro/task.h"
#include <cassert>
#include <memory>
#include <sys/syscall.h>
#include <unistd.h>

using namespace koro;

int main()
{
	int fds[2];
	::pipe(fds);
	int readFd = fds[0];
	int writeFd = fds[1];

	auto ch = iom.bindTaskQueue(readFd);

	bool ret = iom.registerEvent(
		ch, koro::IOManager::Event::kRead,
		std::make_shared<ScheduledTask>(
			[ch, readFd, writeFd]
			{
				char buf[10];
				syscall(SYS_read, readFd, buf, sizeof(buf));
				LOG_FATAL << buf;
				auto child_ch = iom.bindTaskQueue(readFd);

				bool ret = iom.registerEvent(
					child_ch, koro::IOManager::Event::kRead,
					std::make_shared<ScheduledTask>(
						[child_ch, readFd]
						{
							char buf[10];
							syscall(SYS_read, readFd, buf, sizeof(buf));
							LOG_FATAL << buf;
							iom.unregisterEvent(child_ch,
												koro::IOManager::Event::kRead);
						}));
				assert(ret);
				iom.unregisterEvent(ch, koro::IOManager::Event::kRead);

				syscall(SYS_write, writeFd, " world", 7);
			}));
	assert(ret);

	syscall(SYS_write, writeFd, "hello", 6);

	iom.stop();

	::close(readFd);
	::close(writeFd);
}