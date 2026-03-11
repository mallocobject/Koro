#ifndef KORO_CONTEXT_H
#define KORO_CONTEXT_H

#include "koro/channel.h"
#include "koro/io_manager.h"
#include "koro/noncopyable.h"
#include <cstddef>
namespace koro
{
struct Context : public noncopyable
{
	ChannelTable ctable;
	IOManager iom;

  private:
	explicit Context(size_t thread_count = 8) : iom(thread_count)
	{
	}

	static Context& context()
	{
		static Context ctx{8};
		return ctx;
	}

  public:
	static ChannelTable& channel_table()
	{
		return context().ctable;
	}

	static IOManager& io_manager()
	{
		return context().iom;
	}
};
} // namespace koro

#endif