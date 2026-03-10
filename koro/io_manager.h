#ifndef KORO_IO_MANAGER_H
#define KORO_IO_MANAGER_H

#include "koro/scheduler.h"
#include <cstddef>
#include <cstdint>
#include <memory>
namespace koro
{
class Channel;
class ScheduledTask;
class IOManager : public Scheduler
{
	using function = std::function<void()>;

  public:
	enum class Event : uint32_t
	{
		kRead = 0x001,
		kWrite = 0x004,
	};

  public:
	explicit IOManager(size_t thread_count = 1);
	~IOManager();

	std::shared_ptr<Channel> bindTaskQueue(int fd);

	bool registerEvent(std::shared_ptr<Channel> ch, Event e,
					   std::shared_ptr<ScheduledTask> cb, bool useET = false,
					   int timeout = -1);
	void removeChannel(std::shared_ptr<Channel> ch);

	void unregisterEvent(std::shared_ptr<Channel> ch, Event e);
	// void unregisterEventAfterDone(Channel* ch, Event e); // deprecate

  protected:
	void onInit() override;
	void idle(size_t idx) override;
	void tickle(size_t idx) override;
	void handleError(int fd);
};
} // namespace koro

#endif