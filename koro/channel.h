#ifndef KORO_CHANNEL_H
#define KORO_CHANNEL_H

#include "koro/noncopyable.h"
#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sys/epoll.h>
#include <utility>
#include <vector>
namespace koro
{
class TaskQueue;
class Channel : public noncopyable
{
  private:
	int fd_{-1};
	TaskQueue* task_queue_{nullptr};
	uint32_t events_{0};
	uint32_t revents_{0};
	bool in_epoll_{false};

	std::function<void()> read_callback_;
	std::function<void()> write_callback_;
	std::function<void()> error_callback_;

	std::function<void()> on_time_callback_;

  public:
	std::atomic<bool> user_non_block_{false};
	std::atomic<bool> sys_non_block_{false};

  public:
	std::mutex mtx_;

  public:
	Channel(int fd, TaskQueue* task_queue);
	~Channel();

	void remove();

	int fd() const
	{
		return fd_;
	}

	void setInEpoll(bool in_epoll)
	{
		in_epoll_ = in_epoll;
	}

	bool inEpoll() const
	{
		return in_epoll_;
	}

	TaskQueue* taskQueue() const
	{
		return task_queue_;
	}

	void setEvents(uint32_t events)
	{
		events_ = events;
	}

	uint32_t events() const
	{
		return events_;
	}

	void setReadyEvent(uint32_t revents)
	{
		revents_ = revents;
	}

	void useET()
	{
		events_ |= EPOLLET;
	}

	void enableReading()
	{
		events_ |= EPOLLIN;
		update();
	}

	void enableWriting()
	{
		events_ |= EPOLLOUT;
		update();
	}

	void disableReading()
	{
		events_ &= ~EPOLLIN;
		update();
	}

	void disableWriting()
	{
		events_ &= ~EPOLLOUT;
		update();
	}

	void disableAll()
	{
		events_ = 0;
		update();
	}

	bool writing() const
	{
		return events_ & EPOLLOUT;
	}

	void setReadCallback(std::function<void()> cb)
	{
		read_callback_ = std::move(cb);
	}

	void setWriteCallback(std::function<void()> cb)
	{
		write_callback_ = std::move(cb);
	}

	void setErrorCallback(std::function<void()> cb)
	{
		error_callback_ = std::move(cb);
	}

	void setOnTimeCallback(std::function<void()> cb)
	{
		on_time_callback_ = std::move(cb);
	}

	void handleEvent();

	void onTime()
	{
		if (on_time_callback_)
		{
			on_time_callback_();
		}
	}

	void update();

	void triggerEvent(uint32_t e);
};

struct ChannelTable : public noncopyable
{
	std::shared_mutex mtx;
	std::vector<std::shared_ptr<Channel>> chs;
};

extern ChannelTable& ctable;
} // namespace koro

#endif