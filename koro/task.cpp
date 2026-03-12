#include "koro/task.h"
#include "koro/channel.h"
#include "koro/epoll_poller.h"
#include "koro/hook.h"
#include "koro/timer_manager.h"

using namespace koro;

TaskQueue::TaskQueue()
{
}

TaskQueue::~TaskQueue()
{
	if (wakeup_ch_)
	{
		int fd = wakeup_ch_->fd();
		wakeup_ch_->remove();
		wakeup_ch_.reset();
		close(fd);
	}
}