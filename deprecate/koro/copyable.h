#ifndef KORO_COPYABLE_H
#define KORO_COPYABLE_H

namespace koro
{
class copyable
{
  protected:
	copyable() = default;
	~copyable() = default;

	copyable(const copyable&) = default;
	copyable& operator=(const copyable&) = default;
};
} // namespace koro

#endif