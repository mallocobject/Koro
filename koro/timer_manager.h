#ifndef KORO_TIMER_MANAGER_H
#define KORO_TIMER_MANAGER_H

#include "koro/noncopyable.h"
#include "koro/timer_id.h"
#include "koro/timestamp.h"
#include <functional>
#include <memory>
#include <set>
#include <utility>
#include <vector>
namespace koro
{
class Timer;
class Channel;
class TaskQueue;
class TimerManager : public noncopyable
{
  private:
	using Entry = std::pair<Timestamp, std::shared_ptr<Timer>>;

  private:
	std::unique_ptr<Channel> ch_;
	std::set<Entry> timers_;
	std::vector<Entry> active_timers_;
	TaskQueue* task_queue_; // 定时器队列绑定队列

  public:
	explicit TimerManager(TaskQueue* task_queue);
	~TimerManager();

	TimerId registerTimer(Timestamp timestamp, const std::function<void()>& cb,
						  double interval);
	void unregisterEvent(TimerId timer_id);

	Channel* ch() const
	{
		return ch_.get();
	}

  private:
	void triggerTimer();
	bool insert(std::shared_ptr<Timer> timer);
	void resetTimer();
	void resetTimerFd(std::shared_ptr<Timer> timer);
};
} // namespace koro

#endif