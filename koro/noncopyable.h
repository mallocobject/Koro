#ifndef KORO_NONCOPYABLE_H
#define KORO_NONCOPYABLE_H

namespace koro
{
class noncopyable
{
  protected:
	noncopyable() = default;
	~noncopyable() = default;

	noncopyable(const noncopyable&) = delete;
	noncopyable& operator=(const noncopyable&) = delete;
};
} // namespace koro

#endif