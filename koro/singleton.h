#ifndef KORO_SINGLETON_H
#define KORO_SINGLETON_H

namespace koro
{
template <typename Derived> class Singleton
{
  protected:
	Singleton() = default;
	~Singleton() = default;

	Singleton(const Singleton&) = delete;
	Singleton& operator=(const Singleton&) = delete;

  public:
	static Derived& instance()
	{
		static Derived instance;
		return instance;
	}
};
} // namespace koro

#endif