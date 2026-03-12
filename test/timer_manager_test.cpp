#include "elog/logger.h"
#include "koro/io_manager.h"
#include "koro/timer_id.h"

using namespace koro;

int main()
{
	TimerId tid;
	TimerId tid2;
	auto task = [&]
	{
		tid = iom.runEvery(2, [] { LOG_INFO << "hello world"; });
		tid2 = iom.runAfter(10,
							[&]
							{
								LOG_WARN << "cancell timer";
								iom.cancellTimer(tid);
								iom.cancellTimer(tid2);
							});
	};

	iom.submit(task);

	iom.stop();
}