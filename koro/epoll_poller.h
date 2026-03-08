#ifndef KORO_EPOLL_POLLER_H
#define KORO_EPOLL_POLLER_H

#include "koro/noncopyable.h"
#include <cstddef>
#include <sys/epoll.h>
#include <vector>
namespace koro
{
class Channel;
class EpollPoller : public noncopyable
{
	static const size_t kInitEventSize;

  private:
	int epfd_;
	std::vector<epoll_event> evs_;

  public:
	EpollPoller();
	~EpollPoller();

	void updateChannel(Channel* ch);
	void removeChannel(Channel* ch);

	void poll(std::vector<Channel*>* active_chs, int timeout = -1);
};
} // namespace koro

#endif