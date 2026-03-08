#include "koro/epoll_poller.h"
#include "elog/logger.h"
#include "koro/channel.h"
#include "koro/file_descriptor.h"
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <sys/epoll.h>
#include <unistd.h>
#include <vector>

using namespace koro;

const size_t EpollPoller::kInitEventSize = 1024;

EpollPoller::EpollPoller()
{
	epfd_ = FD::createEpollFd();

	evs_.resize(kInitEventSize);
}

EpollPoller::~EpollPoller()
{
	FD::close(epfd_);
}

void EpollPoller::updateChannel(Channel* ch)
{
	epoll_event ev;
	ev.events = ch->events();
	ev.data.ptr = ch;

	if (ch->inEpoll())
	{
		if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, ch->fd(), &ev))
		{
			LOG_ERROR << "failed to modify fd: " << ch->fd();
		}
	}
	else
	{
		if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, ch->fd(), &ev))
		{
			LOG_ERROR << "failed to add fd: " << ch->fd();
			return;
		}
		ch->setInEpoll(true);
	}
}

void EpollPoller::removeChannel(Channel* ch)
{
	assert(ch->inEpoll());
	if (::epoll_ctl(epfd_, EPOLL_CTL_DEL, ch->fd(), nullptr))
	{
		LOG_ERROR << "failed to delete fd: " << ch->fd();
		return;
	}
	ch->setInEpoll(false);
}

void EpollPoller::poll(std::vector<Channel*>* active_chs, int timeout)
{
	int nevs = ::epoll_wait(epfd_, evs_.data(), evs_.size(), timeout);

	if (nevs == -1)
	{
		if (errno != EINTR)
		{
			LOG_ERROR << "wait epoll failed: " << ::strerror(errno);
		}
		return;
	}
	else if (nevs == 0)
	{
		LOG_WARN << "wait epoll out time";
		return;
	}

	for (size_t i = 0; i < nevs; i++)
	{
		Channel* ch = reinterpret_cast<Channel*>(evs_[i].data.ptr);
		ch->setReadyEvent(evs_[i].events);
		active_chs->push_back(ch);
	}

	if (nevs == evs_.size())
	{
		evs_.resize(evs_.size() * 2);
	}
}