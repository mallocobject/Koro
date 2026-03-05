#ifndef KORO_IO_MANAGER_H
#define KORO_IO_MANAGER_H

#include "koro/inplace_function.hpp"
#include "koro/scheduler.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
namespace koro
{
class Fiber;
class IOManager : public Scheduler
{
	using function = InplaceFunction<64>;

  public:
	enum class Event : uint8_t
	{
		kNone = 0x000,
		kRead = 0x001,	// EPOLLIN
		kWrite = 0x004, // EPOLLOUT
	};

  private:
	struct FdContext
	{
		struct EventContext
		{
			Scheduler* scheduler{nullptr};
			std::shared_ptr<Fiber> fiber;
			function cb;
		};

		EventContext read_ctx;
		EventContext write_ctx;
		int fd{-1};
		Event events{Event::kNone};

		EventContext& eventContext(Event event);
		static void resetEventContext(EventContext& event_ctx);
		void triggerEvent(Event event);
	};

  private:
	int epfd_{-1};
	int wakeup_fd_{-1};
	std::atomic<size_t> pending_event_count{0};
	std::mutex mtx_;
	std::map<int, FdContext*> fd_ctxes_;

  public:
	IOManager(size_t thread_count);
	~IOManager();

	bool addEvent(int fd, Event event, function cb = nullptr);
	bool cancelEvent(int fd, Event event);
	bool cancelEventAfterDone(int fd, Event event);
	bool cancelEventAfterDone(int fd);

	static IOManager* localStance();

  protected:
	void tickle() override;
	void idle() override;
	bool stopping() override;

	void resetTimer();

  private:
	bool cancelEventImpl(FdContext* fd_ctx, Event new_events);
	void wakeup();
	void handleRead(); // for wakeup
};
} // namespace koro

inline constexpr koro::IOManager::Event operator&(
	const koro::IOManager::Event& lhs, const koro::IOManager::Event& rhs)
{
	return koro::IOManager::Event(static_cast<uint8_t>(lhs) &
								  static_cast<uint8_t>(rhs));
}

inline constexpr koro::IOManager::Event operator&(
	const koro::IOManager::Event& lhs, uint8_t rhs)
{
	return koro::IOManager::Event(static_cast<uint8_t>(lhs) & rhs);
}

inline constexpr koro::IOManager::Event operator|(
	const koro::IOManager::Event& lhs, const koro::IOManager::Event& rhs)
{
	return koro::IOManager::Event(static_cast<uint8_t>(lhs) |
								  static_cast<uint8_t>(rhs));
}

inline constexpr koro::IOManager::Event operator|(
	uint8_t lhs, const koro::IOManager::Event& rhs)
{
	return koro::IOManager::Event(lhs | static_cast<uint8_t>(rhs));
}

inline constexpr koro::IOManager::Event operator~(
	const koro::IOManager::Event& rhs)
{
	return koro::IOManager::Event(~static_cast<uint8_t>(rhs));
}

inline constexpr bool operator!(koro::IOManager::Event e) noexcept
{
	return e == koro::IOManager::Event::kNone;
}

#endif